FROM java:8-alpine
MAINTAINER Brian Murphy-Dye <bmurphydye@summit.com>

ADD target/uberjar/rosetta.jar /rosetta/app.jar

EXPOSE 3000

CMD ["java", "-jar", "/rosetta/app.jar"]
