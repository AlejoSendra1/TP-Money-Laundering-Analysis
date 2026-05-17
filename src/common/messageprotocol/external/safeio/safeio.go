package safeio

import "io"

func ReadAll(reader io.Reader, size uint32) ([]byte, error) {
	buf := make([]byte, size)
	nread, err := io.ReadFull(reader, buf)
	if err != nil || uint32(nread) < size {
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}

func WriteAll(writer io.Writer, msg []byte) error {
	nwritten_acum := len(msg)
	for nwritten_acum > 0 {
		nwritten, err := writer.Write(msg)
		if err != nil {
			return err
		}
		nwritten_acum -= nwritten
	}
	return nil
}
