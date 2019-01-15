# Decision log for plain text extraction service

## v0.3

### 15 January, 2019: Switch from S3 storage to filesystem-based storage

All kinds of users are interested in plain text content. We need this for QA
processes during submission & moderation, but we also want to provide plain
text content for API clients after announcement. To support text mining
use-cases, we also want to be able to handle requests for large chunks of
content. It can get pretty expensive to support all of those use-cases if
storage costs are linked to individual object access. Filesystems are nice
because they are pretty inexpensive to traverse. Storage costs for plain text
are comparatively low.
