package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/endpoints"
)

func main() {

	// maxGoroutines := runtime.NumCPU() - 1
	// guard := make(chan struct{}, maxGoroutines)

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("unable to load SDK config, " + err.Error())
	}
	cfg.Region = endpoints.UsWest2RegionID
	svc := s3.New(cfg)

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String("fh-pi-paguirigan-a"), // FIXME unhardcode
		MaxKeys: aws.Int64(999999999),
		Prefix:  aws.String("SR/ngs/illumina/apaguiri/1151020_SN367_0568_BHFHN2BCXX-Restore/Unaligned/Project_apaguiri"), // FIXME unhardcode - only set if arg is present
	}

	var keys []*string
	requests := 0

	fmt.Println("about to enter loop")
	for {
		req := svc.ListObjectsV2Request(input)
		result, err := req.Send()
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchBucket:
					fmt.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
				default:
					fmt.Println(aerr.Error())
					return
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
				return
			}

		}

		for _, item := range result.Contents {
			keys = append(keys, item.Key)
		}

		fmt.Println(requests)
		requests++

		if *result.IsTruncated {
			input.ContinuationToken = result.NextContinuationToken
		} else {
			break
		}

	}

	fmt.Println("# of requests:", requests)
	fmt.Println("length of keys:", len(keys))

	// first non-parallel approach (hopefully concurrent though)

	var keymap map[string][]s3.Tag
	keymap = make(map[string][]s3.Tag)

	var keynames map[string]int
	keynames = make(map[string]int)

	for _, key := range keys {
		tagInput := &s3.GetObjectTaggingInput{
			Bucket: aws.String("fh-pi-paguirigan-a"), // FIXME unhardcode
			Key:    key,
		}
		tagReq := svc.GetObjectTaggingRequest(tagInput)
		tagResult, err := tagReq.Send()
		if err != nil {
			panic(err)
		}

		for _, tag := range tagResult.TagSet {
			keynames[*tag.Key] = 1
		}
		keymap[*key] = tagResult.TagSet
	}

	fmt.Println(keymap)
	fmt.Println("keynames:")
	fmt.Println(keynames)
	// TODO make a csv out of keymap
	// where the first column is the key, and the remaining columns are the tags
	// the full set of columns (minus the first) is found in the keys of keynames

}
