================================================================================
  LingSync-to-OLD Migrator
================================================================================

This is a command-line utility that migrates a LingSync corpus to an Online
Linguistic Database (OLD). Both the source LingSync corpus and the destination
OLD must be accessible at URLs (possibly local) via HTTP.


Warnings/disclaimers
--------------------------------------------------------------------------------

- DEPENDENCY: requires that the Python Requests library be installed. All other
  imports are from the standard library.

- It is assumed that the destination OLD is empty. Migrating a LingSync corpus
  to an OLD that already has data in it may result in errors or corrupted data.

- Some LingSync data points (entire documents or specific fields/attributes)
  are purposefully not migrated. You will need to check the resulting OLD to
  verify that the conversion is satisfactory.


Usage
--------------------------------------------------------------------------------

Just run `lingsync2old.py` and you will be prompted for the required arguments::

    $ ./lingsync2old.py

You can also supploy the required arguments as options::

    $ ./lingsync2old.py \
            --ls-url=https://corpus.lingsync.org \
            --ls-corpus=my-lingsync-corpus-name \
            --ls-username=my-lingsync-username \
            --ls-password=my-lingsync-password \
            --old-url=my-old-url \
            --old-username=my-old-username \
            --old-password=my-old-password

Full param/option listing::

    --force-download: boolean that, when `True`, forces the downloading of the
        LingSync/CouchDB data, even if we have already downloaded it. Default
        is `False`.

    --force-convert: boolean that, when `True`, forces the converting of the
        LingSync JSON data to OLD JSON data, even if we have already converted
        it. Default is `False`.

    --force-file-download: boolean that, when `True`, forces the downloading of
        a LingSync file (e.g., audio), even if we have already downloaded and
        saved it.

    --verbose: boolean that makes this script say more about what it's doing.

    --ls-url: The LingSync CouchDB URL that we can make requests to for
        extracting the LingSync data. Defaults to 'https://corpus.lingsync.org'.

    --ls-corpus: The name of the LingSync corpus that we want to
        migrate.

    --ls-username: The username of a user who has sufficient privileges to
        request the LingSync corpus' data from the CouchDB API.

    --ls-password: The password corresponding to the LingSync
        username.

    --old-url: The OLD URL that we will upload the converted LingSync
        data to.

    --old-username: The username of a user on the destination OLD who
        has sufficient privileges to make create, update and delete requests,
        i.e., an admin or a contributor.

    --old-password: The password corresponding to the OLD username.


Algorithm
--------------------------------------------------------------------------------

It's essentially a three-step algorithm:

1. Download. Request LingSync data as JSON using the CouchDB API (and save it
   locally).

2. Convert. Build a JSON structure (from 1) that the OLD can digest (and save it
   locally).

3. Upload. Use the output of (2) to send JSON/REST POST requests to the relevant
   OLD web service.

Here is the general mapping from LingSync documents (or implicit entities) to
OLD resources.

+------------+-------------+
| LingSync   | OLD         |
+============+=============+
| tags       | tags        |
+------------+-------------+
| users      | users       |
+------------+-------------+
| speakers   | speakers    |
+------------+-------------+
| files      | files       |
+------------+-------------+
| datums     | forms       |
+------------+-------------+
| datalists  | corpora     |
+------------+-------------+
| sessions   | collections |
+------------+-------------+


Questions
--------------------------------------------------------------------------------

1. Are there tags in LingSync sessions?

2. Are there files in LingSync sessions?

3. Should we fill in empty values with the values of other attributes. E.g., if
   the morpheme_break value is empty, should the transcription value be copied
   to it?


TODOs
--------------------------------------------------------------------------------

- large file (> 20MB) upload to OLD still not implemented.

- downloading LingSync image files still not implemented.

- make this script sensitive to OLD versions, and maybe to LingSync ones too.


