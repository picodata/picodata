%define use_cmake3 0%{?rhel} == 7

Name: picodata
# ${major}.${major}.${minor}.${patch}, e.g. 1.0.0.35
Version: 1.0.2.0
Release: 1%{?dist}
Epoch: 1

Summary: In-memory database and Rust application server
License: BSD
Group: Databases

Url: http://picodata.io
Source0: %name-%version.tar.gz

%if %use_cmake3
BuildRequires: cmake3
%endif

%if 0%{?rhel} == 8
BuildRequires: libstdc++-static
%endif

Requires: tarantool > 2.8

%description
Picodata is a high performance in-memory NoSQL database and Rust
application server. Picodata supports replication, online backup and
stored procedures in Rust.

This package provides the repository binary and tools

%prep
%setup -q -n %{name}-%{version}

%build
make install-cargo

%if %use_cmake3
make centos7-cmake3
%endif

make build

%install

%if %{?_build_vendor} == alt
%makeinstall_std
%else
%make_install
%endif

%check

%clean
%__rm -rf %{buildroot}

%files
%{_bindir}/picodata
%doc README.md
%{!?_licensedir:%global license %doc}
%if %{?_build_vendor} == alt
%doc license/EE_EN.txt license/EE_RU.txt AUTHORS
%else
%license license/EE_EN.txt license/EE_RU.txt AUTHORS
%endif

%changelog
* Fri Jul  8 2022 <kdy@picodata.io> - 22.07.0%{?dist}
   - Added Picodata product

* Thu Jun  9 2022 <kdy@picodata.io> - 1.0.1.1%{?dist}
   - Tarantool 2.8 and upper

* Fri Feb  4 2022 <kdy@picodata.io> - 1.0.1.0%{?dist}
   - Tarantool 2.8.3 + libicu 69.1

* Thu Dec  2 2021 <kdy@picodata.io> - 1.0.0.35%{?dist}
   - Initial package.
