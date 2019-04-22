

# S3 Policy for Fulltext

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowPutS3Fulltext",
            "Effect": "Allow",
            "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:DeleteObject"],
            "Resource": ["arn:aws:s3:::fulltext/*", "arn:aws:s3:::fulltext", "arn:aws:s3:::fulltext-submission/*", "arn:aws:s3:::fulltext-submission"]
        }
    ]
}


# Kinesis policy for fulltext agent

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowKinesisReadFulltext",
            "Effect": "Allow",
            "Action": [
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:DescribeStream",
                "kinesis:ListTagsForStream"
            ],
            "Resource": "arn:aws:kinesis:*:*:stream/*"
        },
        {
            "Sid": "AllowKinesisDescribeFulltext",
            "Effect": "Allow",
            "Action": "kinesis:DescribeLimits",
            "Resource": "*"
        }
    ]
}
