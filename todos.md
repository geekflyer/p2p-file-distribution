# TODOs

## Potential Improvements

- [ ] Consider adding exponential backoff retry with max attempts before marking piece as failed

  - Currently: failures return last completed piece, main loop retries on next poll (2s)
  - Improvement: track retry count per piece, use exponential backoff (e.g., 1s, 2s, 4s, 8s), mark task as failed after N attempts

- [ ] - maybe bug - currently the tasks assigned to a server are exposed indefinately on the

- gs://test-bucket/models/test-model/test-model.manifest
- gs://pipeline-test-christian/testdata/model.manifest
