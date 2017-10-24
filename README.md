Simple CKAN master-slave sync script written in Python
to sync datasets from one CKAN instance to another.

It syncs:
- Datasets
    - Creates new
    - Deletes those not found in source
    - Syncs all metadata (including custom properties)
    - Syncs resources using reupload
    - Unless the source API key is defined, only public
      datasets are synced
    - Empties trash after sync
- Organizations
    - Descriptions
    - Metadata
    - Logo images

It doesn't sync:
- Pages, blog posts and site descriptions
