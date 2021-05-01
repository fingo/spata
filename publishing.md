Publishing
==========

spata is published from Travis CI,
with help of [sbt-sonatype](https://github.com/xerial/sbt-sonatype) plugin.
A lot of useful information about publishing process may be found in the
Scala's [Library Author Guide](https://docs.scala-lang.org/overviews/contributors/index.html#publish-a-release)
and [sbt documentation](https://www.scala-sbt.org/release/docs/Using-Sonatype.html).

Preparations
-----------------

### Set up [Sonatype account](https://central.sonatype.org/publish/publish-guide/#initial-setup) (for Maven Central)

* Create personal Sonatype repository account through [Sonatype JIRA](https://issues.sonatype.org/secure/Signup!default.jspa).
* [Open a ticket](https://issues.sonatype.org/secure/CreateIssue.jspa?issuetype=21&pid=10134) to set up new project
  and claim access to `fingo.info` domain.
* Log in to [Sonatype](https://oss.sonatype.org/) and create a user token (Profile / User Token).

### Create [PGP keys](https://github.com/sbt/sbt-pgp) to sign releases

* Install [GnuGP](https://gnupg.org/download/index.html) if required.
* Generate a key pair: `gpg --gen-key`:
  * provide `spata bot` as name,
  * Provide your personal email (optimally in `fingo.info` domain).
* Note the key id for later use.
* Check keys: `gpg --list-keys`.
* Publish the key: `gpg --keyserver hkp://ipv4.pool.sks-keyservers.net --send-keys <key-id>`
  (it may take a while until the keys are publicly available).

### Configure [Travis CI](https://travis-ci.com/github/fingo/spata)

* Install [Travis client](https://github.com/travis-ci/travis.rb#installation) if required.
* Set the repository name: `REPO=fingo/spata`.
* [Create GitHub OAuth token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token)
  for Travis CI (with `repo`, `user:email` and `read:org` scopes).
* Log in to Travis CI: `travis login --github-token <token>`.
* Export public key for CI: `gpg -a --export <key-id> ci/public-key.asc`.
* Export private key: `gpg --export-secret-keys --armor <key-id> > target/secret-key.asc`.
* Encrypt private key and send to Travis: `travis encrypt-file target/secret-key.asc --com -r $REPO`
* Adjust `.travis.yml`: replace `$encrypted_<id>_key` and `$encrypted_<id>_iv` with values returned by above command
  (`openssl` call in `publish` stage).
* Move encrypted key to `ci` folder: `mv secret-key.asc.enc ci`.
* Remove exported private key: `rm target/secret-key.asc`.
* Encrypt credentials:
  ** Execute `travis encrypt PGP_PASSPHRASE=<spata_bot_pgp_pass>`
  and replace secure environment variable for `PGP_PASSPHRASE` in `.travis.yml` with returned value.
  ** Execute `travis encrypt SONATYPE_USERNAME=<user_token_name>`
  and replace secure environment variable for `PGP_PASSPHRASE` in `.travis.yml` with returned value.
  ** Execute `travis encrypt SONATYPE_PASSWORD=<user_token_pass>`
  and replace secure environment variable for `PGP_PASSPHRASE` in `.travis.yml` with returned value.
* Logout from Travis: `travis logout`.

Cutting a release
-----------------

A release process uses [sbt-dynver](https://github.com/dwijnand/sbt-dynver) and is triggered by Git tag:
* Set a tag: `git tag -a v<version> -m <info>`.
* Push it: `git push --tags`.

After this, draft a new release in [GitHub](https://github.com/fingo/spata/releases).

Revoking keys
=============

In case of compromised PGP private key, revoke it with following procedure:
* List keys: `gpg --list-keys`.
* Look up the key on server: `gpg --keyserver hkp://ipv4.pool.sks-keyservers.net --search-keys <key-id>`.
* Revoke key: `gpg --output revoke.asc --gen-revoke <key-id>`.
* Import revoked key to keychain: `gpg --import revoke.asc`.
* Publish revoke information: `gpg --keyserver hkp://ipv4.pool.sks-keyservers.net --send-keys <key-id>
  and set up publishing configuration again.
