Installing pgcopydb
===================

There are two ways to install pgcopydb: pull a container image, or build from
source.

Docker Images
-------------

Docker images are published to the GitHub container registry at
`ghcr.io/planetscale/pgcopydb`__. Each commit to the `main` branch publishes
the ``latest`` tag, and each version tag publishes its own ``X.Y.Z`` tag, with
no leading ``v``. The images are built for ``linux/amd64`` and
``linux/arm64``.

To use a tagged release::

  $ docker run --rm -it ghcr.io/planetscale/pgcopydb:0.19.0 pgcopydb --version

Or to follow the main branch::

  $ docker pull ghcr.io/planetscale/pgcopydb:latest
  $ docker run --rm -it ghcr.io/planetscale/pgcopydb:latest pgcopydb --version
  $ docker run --rm -it ghcr.io/planetscale/pgcopydb:latest pgcopydb --help

__ https://github.com/planetscale/pgcopydb/pkgs/container/pgcopydb


Build from sources
------------------

Building from source requires a list of build-dependencies that's comparable
to that of Postgres itself. The pgcopydb source code is written in C and the
build process uses a GNU Makefile.

See our main `Dockerfile`__ for a complete recipe to build pgcopydb as a
debian package when using a debian environment.

__ https://github.com/planetscale/pgcopydb/blob/main/Dockerfile

In particular, the following build dependencies are required to build
pgcopydb. The list is long, because pgcopydb requires a lot of the same
packages as Postgres itself.

On Ubuntu 24.04, with the PostgreSQL 18 server development package::

  $ apt-get install -y \
      postgresql-client-18 \
      postgresql-18 \
      postgresql-server-dev-18

  $ apt-get install -y \
      build-essential \
      git \
      libssl-dev \
      libpq-dev \
      libgc-dev \
      liblz4-dev \
      libpam0g-dev \
      libxml2-dev \
      libxslt1-dev \
      libreadline-dev \
      zlib1g-dev \
      libncurses5-dev \
      libkrb5-dev \
      libselinux1-dev \
      libzstd-dev \
      libnuma-dev

Replace ``18`` with the major version of the Postgres client you want the
binary to carry. A newer client reads older servers, so the newest one you can
install is usually the right choice.

Then the build process is pretty simple, in its simplest form you can just
use ``make clean install``. Put the Postgres binaries on your ``PATH`` first,
so the build finds ``pg_config``::

  $ export PATH=/usr/lib/postgresql/18/bin:$PATH
  $ make -s clean
  $ make -s -j12 install

PlanetScale publishes templates that build a migration host this way, with the
package list above already applied. See the `pgcopydb templates`__ in the
migration-scripts repository, which include an AWS CloudFormation stack that
provisions an instance, builds pgcopydb from a release tag, and installs it.

__ https://github.com/planetscale/migration-scripts/tree/main/pgcopydb-templates

Once you made it this far, it is a good idea to check our `Contribution
Guide`__.

__ https://github.com/planetscale/pgcopydb/blob/main/CONTRIBUTING.md
