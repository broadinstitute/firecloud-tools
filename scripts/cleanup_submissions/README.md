## Delete unused workflows for submissions in a workspace
This workflow can be run from a FireCloud workspace, or locally, to delete workflows that aren't used by the data model (entity attributes) or workspace attributes.

### Inputs:
  `File svcActKeyJson` - Service account credentials JSON, needs write access to the workspace bucket containing submissions.
  `String workspaceProject` - Workspace namespace
  `String workspaceName` - Workspace name
  `Array[String] submissionIds` - Submission IDs to be cleaned up. Optionally, this value can be set to an empty array `[]` if all submission in a workspace should be cleaned up.
  `Boolean dryRun` - When this value is set to false, files from the given submissions will be evaluated and deleted. When this value is set to true, the workflow simply lists the files that are candidates for cleanup.
