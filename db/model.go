package db

type Document struct {
	Body     []byte
	Metadata DocMetadata
	hash     string
}

func NewDocument(body []byte) Document {
	return Document{
		Body:     body,
		Metadata: DocMetadata{}}
}

func (d Document) Hash() string {
	return d.hash
}

type DocMetadata map[string]string

func (m DocMetadata) Set(key string, value string) {
	m[key] = value
}
