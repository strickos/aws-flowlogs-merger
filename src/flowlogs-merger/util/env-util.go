package util

import "os"

/*
Region returns the name of the AWS region to configure the AWS SDK for
*/
func Region() string {
	region, customRegionSpecified := os.LookupEnv("USE_REGION")
	if !customRegionSpecified {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}

	if len(region) == 0 { // If region is not specified, then fallback to the AWS_REGION env variable
		region = os.Getenv("AWS_REGION")
		if len(region) == 0 { // If no AWS_REGION specified, fallback to ultimate default "us-east-1"
			region = "us-east-1"
		}
	}

	return region
}

/*
GetEnvProp returns the value of the given environment variable, or the defaultValue if the environment variable has not been specified
*/
func GetEnvProp(name string, defaultValue string) string {
	val, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	return val
}
