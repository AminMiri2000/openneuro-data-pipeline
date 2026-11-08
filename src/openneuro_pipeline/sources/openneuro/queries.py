"""GraphQL documents sent to OpenNeuro.

Kept apart from the client so queries can be reviewed, versioned and tested on
their own. Only fields the downstream models use are requested: smaller
responses mean faster pages and fewer chances for an unrelated schema change
to break the pipeline.
"""

DATASETS_QUERY = """
query Datasets($first: Int!, $after: String, $modality: String) {
  datasets(first: $first, after: $after, modality: $modality) {
    edges {
      node {
        id
        created
        latestSnapshot {
          id
          tag
          created
          description {
            Name
            Authors
            License
            DatasetDOI
            BIDSVersion
          }
          summary {
            modalities
            subjects
            sessions
            tasks
            size
            totalFiles
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""
