Name:    librdkafka
# NOTE: Make sure to update this to match rdkafka.h version
Version: 0.8.5
Release: 0
%define soname 1

Summary: The Apache Kafka C library
Group:   Development/Libraries/C and C++
License: BSD-2-Clause
URL:     https://github.com/edenhill/librdkafka
Source:	 librdkafka-%{version}.tar.gz

BuildRequires: zlib-devel libstdc++-devel gcc >= 4.1 gcc-c++
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

%description
librdkafka is a C/C++ library implementation of the Apache Kafka protocol, containing both Producer and Consumer support.
It was designed with message delivery reliability and high performance in mind, current figures exceed 800000 msgs/second for the producer and 3 million msgs/second for the consumer.


%package -n %{name}%{soname}
Summary: The Apache Kafka C library
Group:   Development/Libraries/C and C++

%description -n %{name}%{soname}
librdkafka is a C/C++ library implementation of the Apache Kafka protocol, containing both Producer and Consumer support.


%package -n %{name}-devel
Summary: The Apache Kafka C library (Development Environment)
Group:   Development/Libraries/C and C++
Requires: %{name}%{soname} = %{version}

%description -n %{name}-devel
librdkafka is a C/C++ library implementation of the Apache Kafka protocol, containing both Producer and Consumer support.

This package contains headers and libraries required to build applications
using librdkafka.


%prep
%setup -q

%configure

%build
make

%install
rm -rf %{buildroot}
DESTDIR=%{buildroot} make install

%clean
rm -rf %{buildroot}

%post   -n %{name}%{soname} -p /sbin/ldconfig
%postun -n %{name}%{soname} -p /sbin/ldconfig

%files -n %{name}%{soname}
%defattr(444,root,root)
%{_libdir}/librdkafka.so.%{soname}
%{_libdir}/librdkafka++.so.%{soname}
%defattr(-,root,root)
%doc README.md CONFIGURATION.md INTRODUCTION.md
%doc LICENSE LICENSE.pycrc LICENSE.snappy

%defattr(-,root,root)
#%{_bindir}/rdkafka_example
#%{_bindir}/rdkafka_performance


%files -n %{name}-devel
%defattr(-,root,root)
%{_includedir}/librdkafka
%defattr(444,root,root)
%{_libdir}/librdkafka.a
%{_libdir}/librdkafka.so
%{_libdir}/librdkafka++.a
%{_libdir}/librdkafka++.so


%changelog
* Fri Oct 24 2014 Magnus Edenhill <rdkafka@edenhill.se> 0.8.5-0
- 0.8.5 release

* Mon Aug 18 2014 Magnus Edenhill <rdkafka@edenhill.se> 0.8.4-0
- 0.8.4 release

* Mon Mar 17 2014 Magnus Edenhill <vk@edenhill.se> 0.8.3-0
- Initial RPM package
