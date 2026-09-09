# Resolve a KoBo asset id to its Airtable form record id

Reads the frame's `forms` table, the only place the two ids are linked.
Errors unless the asset id matches exactly one record, since an
unresolved id would silently filter every mapping table down to nothing.

## Usage

``` r
get_airtable_form_id(kobo_asset_id = NULL, conf = NULL)
```

## Arguments

- kobo_asset_id:

  A single KoBoToolbox asset id.

- conf:

  The configuration file.

## Value

The Airtable record id, length 1.
