#!/bin/bash

TMPDIRBASE=/tmp

# locate the topdir relative to this script
SCRIPT="`pwd`/$0"
TOPDIR=`dirname $SCRIPT`

# ensure that dist/ directory exists
mkdir -p $TOPDIR/dist
if [ $? -ne 0 ]; then
    echo "Error: $TOPDIR/dist doesn't exist and can't be created"
    exit 1
fi

# build jars
cd $TOPDIR
echo "building from $TOPDIR"
sbt compile package one-jar
if [ $? -ne 0 ]; then
    echo "Error: failed to build from sources"
    exit 1
fi

# extract version from the jar name
JAR=`ls target/scala-2.10/mandelbrot-server_2.10-*.jar |head -n1`
VERSION=`basename $JAR | sed 's/mandelbrot-server_2\.10-\(.*\)\.jar$/\1/'`
echo "creating archives for version $VERSION"

# create tmpdir
TMPDIR=`mktemp -d --tmpdir=$TMPDIRBASE makedist.XXXXXX`
if [ $? -ne 0 ]; then
    echo "Error: failed to create tmpdir"
    exit 1
fi
echo "using tmpdir $TMPDIR"

# create bin tarball
BINPKG="mandelbrot-server-$VERSION-bin"
ln -s $TOPDIR $TMPDIR/$BINPKG
tar -C $TMPDIR -chzv \
    --exclude-vcs \
    --exclude $BINPKG/dist/* \
    --exclude $BINPKG/history/* --exclude $BINPKG/journal/* --exclude $BINPKG/snapshots/* --exclude $BINPKG/state/* \
    --exclude $BINPKG/project/project --exclude $BINPKG/project/target \
    --exclude $BINPKG/target/one-jar-redist --exclude $BINPKG/target/resolution-cache --exclude $BINPKG/target/streams \
    --exclude $BINPKG/target/scala-2.10/cache --exclude $BINPKG/target/scala-2.10/classes \
    -f $TMPDIR/$BINPKG.tar.gz \
    $BINPKG/
cp -f $TMPDIR/$BINPKG.tar.gz $TOPDIR/dist/

# create src tarball
SRCPKG="mandelbrot-server-$VERSION-src"
ln -s $TOPDIR $TMPDIR/$SRCPKG
tar -C $TMPDIR -chzv \
    --exclude-vcs \
    --exclude $SRCPKG/dist/* \
    --exclude $SRCPKG/history/* --exclude $SRCPKG/journal/* --exclude $SRCPKG/snapshots/* --exclude $SRCPKG/state/* \
    --exclude $SRCPKG/target \
    --exclude $SRCPKG/project/project --exclude $SRCPKG/project/target \
    -f $TMPDIR/$SRCPKG.tar.gz \
    $SRCPKG/
cp -f $TMPDIR/$SRCPKG.tar.gz $TOPDIR/dist/

# clean up tmpdir
rm -rf $TMPDIR
