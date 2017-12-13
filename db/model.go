package db

type Document struct {
	Body     []byte
	Metadata DocMetadata
	Hash     string
}

func NewDocument(body []byte) Document {
	return Document{
		Body:     body,
		Metadata: DocMetadata{}}
}

type DocMetadata map[string]string

func (m DocMetadata) Set(key string, value string) {
	m[key] = value
}
