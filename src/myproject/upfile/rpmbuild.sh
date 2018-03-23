#!/bin/bash
echo Building RPMs..
PACKAGENAME='upfile'
GITROOT=`git rev-parse --show-toplevel`
cd $GITROOT
VER=1.0
COMMITVER='HEAD'

if [ $# -gt 0 ]; then
	echo 'build specified version:' $1
	COMMITVER=$1
fi

REL=`git rev-parse --short $COMMITVER`git
REL=`git log --oneline|wc -l`.$REL
TOPDIR=$GITROOT/src/myproject/$PACKAGENAME
RPMTOPDIR=$TOPDIR/rpm-build
echo "Ver: $VER, Release: $REL, RPMTOPDIR: $RPMTOPDIR, TOPDIR: $TOPDIR"
cd $TOPDIR

# Create tarball
#mkdir -p $RPMTOPDIR/{SOURCES,SPECS}
#git archive --format=tar --prefix=${PACKAGENAME}-${VER}-${REL}/ $COMMITVER | gzip -c > $RPMTOPDIR/SOURCES/${PACKAGENAME}-${VER}-${REL}.tar.gz
#tar -zcvf $RPMTOPDIR/SOURCES/${PACKAGENAME}-${VER}-${REL}.tar.gz $TOPDIR/src $TOPDIR/conf $TOPDIR/bin

#exit

# Convert git log to RPM's ChangeLog format (shown with rpm -qp --changelog <rpm file>)
sed -e "s/%{ver}/$VER/" -e "s/%{rel}/$REL/" $TOPDIR/package/${PACKAGENAME}.spec > $TOPDIR/${PACKAGENAME}.spec
git log --format="* %cd %aN%n- (%h) %s%d%n" --date=local | sed -r 's/[0-9]+:[0-9]+:[0-9]+ //' >> $TOPDIR/${PACKAGENAME}.spec
# Build SRC and binary RPMs
#rpmbuild \
	#--define "_topdir $TOPDIR" \
	#--define "_rpmdir $PWD" \
	#--define "_srcrpmdir $PWD" \
	#--define '_rpmfilename %%{PACKAGENAME}-%%{VER}-%%{REL}.%%{ARCH}.rpm' \
	#-ba $TOPDIR/${PACKAGENAME}.spec &&
	#rm -rf $RPMTOPDIR
echo Done
