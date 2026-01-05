%global _lto_cflags %{_lto_cflags} -Wno-lto-type-mismatch

# Turn off strip'ng of binaries
%if "%{?_build_vendor}" == "alt"
%global __find_debuginfo_files %nil
%brp_strip_none %_bindir/*
%else
%define debug_package %{nil}
%global __strip /bin/true
%endif

%if 0%{?fedora} >= 33
%global build_ldflags -Wl,-z,relro -Wl,--as-needed  -Wl,-z,now -specs=/usr/lib/rpm/redhat/redhat-annobin-cc1  -Wl,--build-id=sha1   -Wa,--noexecstack -Wa,--generate-missing-build-notes=yes -DPURIFY
%endif

%define use_cmake3 0%{?rhel} == 7
%define use_dynamic_build 0%{getenv:USE_DYNAMIC_BUILD} == 1

Name: picodata
# ${major}.${major}.${minor}.${patch}, e.g. 1.0.0.35
Version: 1.0.2.0
Release: 1%{?dist}
Epoch: 1

Vendor: Picodata
Summary: In-memory database and Rust application server
License: BSD
Group: Databases

Url: http://picodata.io
Source0: %name-%version.tar.gz

%if %use_cmake3
BuildRequires: cmake3
%endif

%if %use_dynamic_build
BuildRequires: zlib-devel
%if "%{?_build_vendor}" == "alt"
BuildRequires: libluajit-devel
BuildRequires: libsasl2-devel
BuildRequires: libldap-devel
%else
BuildRequires: luajit-devel
BuildRequires: cyrus-sasl-devel
BuildRequires: openldap-devel
BuildRequires: libstdc++-devel
%endif

BuildRequires: libunwind-devel
%else
%if 0%{?rhel} == 8 || 0%{?redos} > 0 || 0%{?fedora} >= 33
BuildRequires: libstdc++-static
%endif
%endif


%if "%{?mandriva_os}" == "linux"
BuildRequires: lib64luajit-5.1-devel
BuildRequires: lib64zstd-devel
BuildRequires: lib64z-devel
BuildRequires: lib64yaml-devel
BuildRequires: lib64curl-devel
BuildRequires: lib64icu-devel
BuildRequires: lib64openssl-devel
BuildRequires: lib64readline-devel
BuildRequires: lib64stdc++-static-devel
BuildRequires: lib64gomp-static-devel
BuildRequires: lib64crypt-devel
%else
BuildRequires: readline-devel
BuildRequires: openssl-devel
BuildRequires: libcurl-devel
BuildRequires: libicu-devel
BuildRequires: libyaml-devel
BuildRequires: libzstd-devel
%endif

%if 0%{?fedora} >= 33
BuildRequires: perl-FindBin
%endif

%if 0%{?rhel} >= 8 || 0%{?redos} > 0 || 0%{?fedora} >= 33 || "%{?mandriva_os}" == "linux"
Recommends:  postgresql
%endif

%description
Picodata is a high performance in-memory NoSQL database and Rust
application server. Picodata supports replication, online backup and
stored procedures in Rust.

This package provides the repository binary and tools
%if "%{?_build_vendor}" == "alt"
%if "%{?dist}" == ".p9"
Recommends:  postgresql12
%endif
%if "%{?dist}" == ".p10"
Recommends:  postgresql16
%endif
%endif

%prep
%setup -q -n %{name}-%{version}

%build
make install-cargo
sudo find {/opt,/usr} -name libgomp.spec -delete

%if %use_cmake3
make centos7-cmake3
%endif

make build-release-pkg

%if %use_dynamic_build
make sbom
mkdir -p %{_rpmdir}
mv -v picodata.cdx.json %{_rpmdir}/../
%endif

%install

%if "%{?_build_vendor}" == "alt"
%makeinstall_std
%else
%make_install
%endif

%check

%clean
%__rm -rf %{buildroot}

%files
%{_bindir}/picodata
%{_bindir}/gostech-audit-log
%doc README.md
%{!?_licensedir:%global license %doc}
%if "%{?_build_vendor}" == "alt"
%doc doc/licenses/eula_en.txt doc/licenses/eula_ru.txt AUTHORS
%else
%license doc/licenses/eula_en.txt doc/licenses/eula_ru.txt AUTHORS
%endif

%changelog
* Mon Dec  5 2022 <kdy@picodata.io> - 22.07.0%{?dist}
   - Replace tarantool to tarantool-picodata

* Fri Jul  8 2022 <kdy@picodata.io> - 22.07.0%{?dist}
   - Added Picodata product

* Thu Jun  9 2022 <kdy@picodata.io> - 1.0.1.1%{?dist}
   - Tarantool 2.8 and upper

* Fri Feb  4 2022 <kdy@picodata.io> - 1.0.1.0%{?dist}
   - Tarantool 2.8.3 + libicu 69.1

* Thu Dec  2 2021 <kdy@picodata.io> - 1.0.0.35%{?dist}
   - Initial package.
