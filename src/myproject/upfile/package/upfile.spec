
%define debug_package %{nil}
%define user_path /usr/local/sandai/upfile
%define upfile_build_path %{_topdir}/BUILD/%{name}-%{version}

Name:           upfile
Version:        %{ver}
Release:        %{rel}%{?dist}
Summary:        This is OneThing CDN streamengine system

Group:          Applications/Server
License:        GPL
URL:            http://xycdn.com
Source0:        %{name}-%{version}.tar.gz
BuildRoot:      %{_topdir}/BUILDROOT/%{name}-%{version}


%description


%prep
%setup -cn %{_topdir}/BUILD/%{name}-%{version}

%build

%install
%{__install} -d $RPM_BUILD_ROOT%{user_path}

cd %{tairclient_build_path
go build -o bin/upfile src/main.go

cp -rf %{upfile_build_path}/bin/* $RPM_BUILD_ROOT%{user_path}
cp -rf %{upfile_build_path}/conf/* $RPM_BUILD_ROOT%{user_path}

%clean
#rm -rf $RPM_BUILD_ROOT
#rm -rf %{_topdir}/BUILD/*

%files
%defattr(-,root,root,-)
/*


%post

%postun

%changelog


