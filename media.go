package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const maxMediaBytes int64 = 2 << 30
const mediaRequestTimeout = 15 * time.Minute

type outboundMedia struct {
	Kind     string `json:"kind" jsonschema:"WhatsApp attachment type: image, audio, video or document"`
	FileName string `json:"file_name" jsonschema:"Base filename of a regular file placed in the service data directory's outgoing folder. Do not supply a URL or path."`
	MIMEType string `json:"mime_type,omitempty" jsonschema:"Optional MIME type, e.g. audio/mp4. Otherwise inferred from the file."`
	Caption  string `json:"caption,omitempty" jsonschema:"Optional caption for an image, video or document (up to 1024 characters). Audio has no caption."`
	SHA256   string `json:"sha256,omitempty" jsonschema:"Optional expected file SHA-256 in lowercase hex. Saved media drafts bind this automatically."`
}

var mediaTypes = map[string]whatsmeow.MediaType{
	"image": whatsmeow.MediaImage, "audio": whatsmeow.MediaAudio,
	"video": whatsmeow.MediaVideo, "document": whatsmeow.MediaDocument,
}

func (m outboundMedia) validate() error {
	if _, ok := mediaTypes[m.Kind]; !ok {
		return errors.New("media.kind must be image, audio, video or document")
	}
	if !filepath.IsLocal(m.FileName) || m.FileName == "." || strings.ContainsAny(m.FileName, "/\\") || strings.IndexFunc(m.FileName, unicode.IsControl) >= 0 {
		return errors.New("media.file_name must be a plain filename in the outgoing directory")
	}
	if len([]rune(m.Caption)) > 1024 || (m.Kind == "audio" && m.Caption != "") {
		return errors.New("captions may contain up to 1024 characters on images, videos and documents")
	}
	if m.SHA256 != "" {
		hash, err := hex.DecodeString(m.SHA256)
		if err != nil || len(hash) != sha256.Size || strings.ToLower(m.SHA256) != m.SHA256 {
			return errors.New("media.sha256 must be a 64-character lowercase hex digest")
		}
	}
	return nil
}

// Pin the directory descriptor before opening the basename. Neither component
// can be swapped for a symlink to the session database or another host file.
func (a *archive) openMedia(name string) (*os.File, error) {
	dir, err := unix.Open(a.mediaDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.New("could not open the local outgoing directory")
	}
	defer unix.Close(dir)
	fd, err := unix.Openat(dir, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.New("attachment is unavailable; place a regular file with this name in the outgoing directory")
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (m outboundMedia) mimeType(prefix []byte) (string, error) {
	sniffed := http.DetectContentType(prefix)
	detected, _, _ := mime.ParseMediaType(sniffed)
	value := m.MIMEType
	if value == "" {
		value = mime.TypeByExtension(strings.ToLower(filepath.Ext(m.FileName)))
		// Ogg, MP4 and WebM can each contain audio or video. The generic
		// container signature alone does not distinguish those two usages.
		if m.Kind == "audio" && (detected == "application/ogg" || detected == "video/mp4" || detected == "video/webm") {
			value = "audio/" + strings.SplitN(detected, "/", 2)[1]
		} else if m.Kind == "video" && detected == "application/ogg" {
			value = "video/ogg"
		} else if m.Kind != "document" && strings.HasPrefix(detected, m.Kind+"/") {
			value = detected
		} else if value == "" {
			value = sniffed
		}
	}
	base, params, err := mime.ParseMediaType(value)
	if err != nil || !strings.Contains(base, "/") || strings.Contains(base, "*") || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", errors.New("invalid media MIME type")
	}
	if m.Kind != "document" {
		if !strings.HasPrefix(base, m.Kind+"/") {
			return "", fmt.Errorf("media.kind %s needs a %s/* MIME type", m.Kind, m.Kind)
		}
		family, _, _ := strings.Cut(detected, "/")
		sharedContainer := m.Kind == "audio" && (detected == "video/mp4" || detected == "video/webm")
		if ((family == "image" || family == "audio" || family == "video") && family != m.Kind && !sharedContainer) ||
			detected == "application/pdf" || detected == "application/zip" {
			return "", errors.New("the file contents do not match the requested media kind")
		}
	}
	return mime.FormatMediaType(base, params), nil
}

type cancellableReader struct {
	ctx context.Context
	r   io.Reader
}

func (r cancellableReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// Resolve metadata and hash the same bytes written to dst. For dispatch dst is
// a private snapshot, so even an in-place edit cannot change an approved upload.
func (a *archive) readMedia(ctx context.Context, m outboundMedia, dst io.Writer) (outboundMedia, error) {
	if err := m.validate(); err != nil {
		return m, err
	}
	f, err := a.openMedia(m.FileName)
	if err != nil {
		return m, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > maxMediaBytes {
		return m, errors.New("attachment must be a nonempty regular file of at most 2 GiB")
	}
	r := bufio.NewReader(f)
	prefix, err := r.Peek(512)
	if err != nil && err != io.EOF {
		return m, errors.New("could not read attachment")
	}
	m.MIMEType, err = m.mimeType(prefix)
	if err != nil {
		return m, err
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(hash, dst), io.LimitReader(cancellableReader{ctx, r}, maxMediaBytes+1))
	if err != nil || n != info.Size() {
		return m, errors.New("could not read a complete, stable attachment")
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if m.SHA256 != "" && m.SHA256 != digest {
		return m, errors.New("attachment contents changed; restore the approved file or draft the replacement")
	}
	m.SHA256 = digest
	return m, nil
}

type uploadedMedia struct {
	media  outboundMedia
	upload whatsmeow.UploadResponse
}

func (b *bridge) uploadMedia(ctx context.Context, in sendInput, job *replyJob) (*uploadedMedia, error) {
	f, err := os.CreateTemp("", "whatsapp-mcp-upload-*")
	if err != nil {
		return nil, errors.New("could not create attachment snapshot")
	}
	defer f.Close()
	// This service runs on Unix. Unlink now so even an abrupt exit frees the snapshot.
	if err = os.Remove(f.Name()); err != nil {
		return nil, errors.New("could not secure attachment snapshot")
	}
	m, err := b.a.readMedia(ctx, *in.Media, f)
	if err != nil {
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, errors.New("could not read attachment snapshot")
	}
	if job != nil {
		if err = b.a.guardReply(ctx, *job, in, "", ""); err != nil {
			return nil, err
		}
	}
	// UploadReader supports writing encrypted blocks back into the same snapshot.
	up, err := b.uploadFn(ctx, cancellableReader{ctx, f}, f, mediaTypes[m.Kind])
	if err != nil {
		return nil, errors.New("attachment upload failed before message dispatch; retry the same request")
	}
	if (up.URL == "" && up.DirectPath == "") || up.FileLength == 0 || len(up.MediaKey) != 32 ||
		len(up.FileEncSHA256) != sha256.Size || hex.EncodeToString(up.FileSHA256) != m.SHA256 {
		return nil, errors.New("attachment upload returned incomplete or mismatched metadata; no message sent")
	}
	return &uploadedMedia{media: m, upload: up}, nil
}

func (u *uploadedMedia) message(ci *waE2E.ContextInfo) *waE2E.Message {
	m, up := u.media, u.upload
	switch m.Kind {
	case "image":
		return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
			Mimetype: &m.MIMEType, Caption: &m.Caption, ContextInfo: ci,
			URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
			FileSHA256: up.FileSHA256, FileEncSHA256: up.FileEncSHA256, FileLength: &up.FileLength,
		}}
	case "audio":
		return &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
			Mimetype: &m.MIMEType, PTT: proto.Bool(false), ContextInfo: ci,
			URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
			FileSHA256: up.FileSHA256, FileEncSHA256: up.FileEncSHA256, FileLength: &up.FileLength,
		}}
	case "video":
		return &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
			Mimetype: &m.MIMEType, Caption: &m.Caption, ContextInfo: ci,
			URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
			FileSHA256: up.FileSHA256, FileEncSHA256: up.FileEncSHA256, FileLength: &up.FileLength, StreamingSidecar: up.StreamingSidecar,
		}}
	default: // document; kind was validated before upload
		doc := &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
			Mimetype: &m.MIMEType, FileName: &m.FileName, Title: &m.FileName, Caption: &m.Caption, ContextInfo: ci,
			URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
			FileSHA256: up.FileSHA256, FileEncSHA256: up.FileEncSHA256, FileLength: &up.FileLength,
		}}
		if m.Caption != "" {
			return &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{Message: doc}}
		}
		return doc
	}
}
