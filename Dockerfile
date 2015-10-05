FROM scratch
MAINTAINER Loggi "dev@loggi.com"
ADD main /
ENTRYPOINT ["/main"]
CMD ["/main", "-service='true'"] 
