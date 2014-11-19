# Real-Time Hot Words within Spanish Tweets using Node.js, Redis and Storm

Realtime geolocation of the hottest words within Spanish tweets.

App build on Java and server build on Node.js with Storm, Redis, Twitter4j, HTML5 Geolocation API, Socket.io and Leaflet maps library.

Redis used as PUB/SUB system. 
Storm helps computing most used words within Spanish tweets in real-time.

![alt tag](https://raw.github.com/MarioCerdan/SpanishTweets/master/TweetsStormNode.png)

### How to

- Install Redis in localhost.
- Npm install node required modules.
- Compile Java app (Storm core) using Maven (see pom.xml for more instructions).
- Run node server.js
- Run Java program to start collecting most used words within Spanish Tweets!

### Requirements

- OS X, Linux, Windows;
- It's tested to run with node v0.8 and v0.10;
- Redis 2.6.16;
- Storm 0.8.1;

---

Special thanks to Dimitri Voronianski and 
http://kaviddiss.com/2013/05/17/how-to-get-started-with-storm-framework-in-5-minutes/


Project for educational purposes.


