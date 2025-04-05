Publishing
==========

spata is published from Github Actions,
with help of [sbt-ci-release](https://github.com/sbt/sbt-ci-release) plugin.
A lot of useful information about publishing process may be found in the
Scala's [Library Author Guide](https://docs.scala-lang.org/overviews/contributors/index.html#publish-a-release)
and [sbt documentation](https://www.scala-sbt.org/release/docs/Using-Sonatype.html).

Preparations
-----------------

### Set up [Sonatype account](https://central.sonatype.org/publish/publish-guide/#initial-setup) (for Maven Central)

*   Create personal Sonatype repository account through [Sonatype JIRA](https://issues.sonatype.org/secure/Signup!default.jspa).

*   [Open a ticket](https://issues.sonatype.org/secure/CreateIssue.jspa?issuetype=21&pid=10134) to set up new project
    and claim access to `fingo.info` domain.

*   Log in to [Sonatype](https://oss.sonatype.org/) and create a user token (Profile / User Token).

### Create [PGP keys](https://github.com/sbt/sbt-pgp) to sign releases

*   Install [GnuGP](https://gnupg.org/download/index.html) if required.

*   Generate a key pair: `gpg --gen-key`:
    *   provide `spata bot` as name,
    *   provide your personal email (optimally in `fingo.info` domain).

*   Note the key id for later use.

*   Check keys: `gpg --list-keys`.

*   Publish the key: `gpg --keyserver hkps://keys.openpgp.org --send-keys <key-id>`
    (it may take a while until the keys are publicly available).

### Configure Github Actions

*   Add Sonatype token (`SONATYPE_USERNAME` and `SONATYPE_PASSWORD`) to
    [Github repository secrets](https://github.com/fingo/spata/settings/secrets/actions)

*   Retrieve the PGP key: `gpg --armor --export-secret-keys <key-id> | base64 | pbcopy`
    (this is MacOS specific command, for other platforms see
    [Library Author Guide](https://docs.scala-lang.org/overviews/contributors/index.html#export-your-pgp-key-pair))

*   Add PGP key (`PGP_SECRET`) to
    [Github repository secrets](https://github.com/fingo/spata/settings/secrets/actions)

*   Add PGP key passphrase (`PGP_PASSPHRASE`) to
    [Github repository secrets](https://github.com/fingo/spata/settings/secrets/actions)

Cutting a release
-----------------

A release process uses [sbt-dynver](https://github.com/dwijnand/sbt-dynver) and is triggered by Git tag:
*   Set a tag: `git tag -a v<version> -m <info>`.
*   Push it: `git push --tags`.

After this, draft a new release in [GitHub](https://github.com/fingo/spata/releases).

Revoking keys
-------------

In case of compromised PGP private key, revoke it with following procedure:

*   List keys: `gpg --list-keys`.

*   Look up the key on server: `gpg --keyserver hkp://ipv4.pool.sks-keyservers.net --search-keys <key-id>`.

*   Revoke key: `gpg --output revoke.asc --gen-revoke <key-id>`.

*   Import revoked key to keychain: `gpg --import revoke.asc`.

*   Publish revoke information: `gpg --keyserver hkp://ipv4.pool.sks-keyservers.net --send-keys <key-id>
    and set up publishing configuration again.
