FROM scratch
MAINTAINER Loggi "dev@loggi.com"
ADD main /
ENTRYPOINT ["/main"]
CMD ["/main", \
#	"-instance-id='instanceid'", \
	"-service='true'", \
	"-fetch-nap='1m'", "-portion-nap='10s'", \
#	"-retrieved-file-dir='/retrieved'" \
	] 
