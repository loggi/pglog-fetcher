package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
)

func main() {

	// some consts
	// TODO seiti - extract to args
	region := "sa-east-1"
	instanceId := "staging-db-rds"
	retries := 10
	maxRecords := int64(2)

	svc := rds.New(aws.NewConfig().WithRegion(region).WithMaxRetries(retries))

	resp, err := listDBLogFiles(svc, instanceId, maxRecords)
	if err != nil {
		errProcessing(err)
	}

	// TODO seiti - remove this
	fmt.Println(resp.DescribeDBLogFiles)
}

func listDBLogFiles(svc *rds.RDS, instanceId string, maxRecords int64) (*rds.DescribeDBLogFilesOutput, error) {
	params := &rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: aws.String(instanceId),
		MaxRecords:           aws.Int64(maxRecords),
	}
	return svc.DescribeDBLogFiles(params)
}

func downloadDBLogFilePortion(svc *rds.RDS, instanceId string, logFilename string, marker string, nlines int64) (*rds.DownloadDBLogFilePortionOutput, error) {
	params := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: aws.String(instanceId),
		LogFileName:          aws.String(logFilename),
		Marker:               aws.String(marker),
		NumberOfLines:        aws.Int64(nlines),
	}
	return svc.DownloadDBLogFilePortion(params)
}

func errProcessing(err error) {
	if awsErr, ok := err.(awserr.Error); ok {
		log.Errorln(awsErr.Code(), awsErr.Message())
		log.WithError(err)
		if origErr := awsErr.OrigErr(); origErr != nil {
			log.WithError(origErr)
		}
	} else {
		log.WithError(err)
	}
}
