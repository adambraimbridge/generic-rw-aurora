FROM alpine:3.6

COPY . /generic-rw-aurora/

RUN apk --update add git go libc-dev ca-certificates \
  && export GOPATH=/gopath \
  && ORG_PATH="github.com/Financial-Times" \
  && PROJECT_NAME="generic-rw-aurora" \
  && mkdir -p $GOPATH/src/${ORG_PATH} \
  && cp -r ${PROJECT_NAME} $GOPATH/src/${ORG_PATH} \
  && cd $GOPATH/src/${ORG_PATH}/${PROJECT_NAME} \
  && BUILDINFO_PACKAGE="${ORG_PATH}/${PROJECT_NAME}/vendor/github.com/Financial-Times/service-status-go/buildinfo." \
  && VERSION="version=$(git describe --tag --always 2> /dev/null)" \
  && DATETIME="dateTime=$(date -u +%Y%m%d%H%M%S)" \
  && REPOSITORY="repository=$(git config --get remote.origin.url)" \
  && REVISION="revision=$(git rev-parse HEAD)" \
  && BUILDER="builder=$(go version)" \
  && LDFLAGS="-X '"${BUILDINFO_PACKAGE}$VERSION"' -X '"${BUILDINFO_PACKAGE}$DATETIME"' -X '"${BUILDINFO_PACKAGE}$REPOSITORY"' -X '"${BUILDINFO_PACKAGE}$REVISION"' -X '"${BUILDINFO_PACKAGE}$BUILDER"'" \
  && echo $LDFLAGS \
  && go get -u github.com/kardianos/govendor \
  && $GOPATH/bin/govendor sync \
  && go build -ldflags="${LDFLAGS}" \
  && mv generic-rw-aurora /generic-rw-aurora-app \
  && apk del go git libc-dev \
  && rm -rf $GOPATH /var/cache/apk/* /generic-rw-aurora

CMD [ "/generic-rw-aurora-app" ]
