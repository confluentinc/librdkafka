f = open('src/rdkafka.h')
for line in f:
    if '#define RD_KAFKA_VERSION' in line:
        version = line.split()[-1]
        break
f.close()

major = int(version[2:4], 16)
minor = int(version[4:6], 16)
patch = int(version[6:8], 16)
version = '.'.join(str(item) for item in (major, minor, patch))

print version
