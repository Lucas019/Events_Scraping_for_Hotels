# Brief Intro
This is a coursework group project, our team leveraged ticketmaster API, positionstack API and eventbrite website to scrape useful events for chain hotels such as [Ace Hotel](https://acehotel.com/). Then we stored the events in two separate MongoDB database, one for ticketmaster and one for eventbrite.

# Business Value
Ace Hotel is a chain of hotels, it operates hotels in multiple big cities across the globe. for illustration purposes, we chose 3 locations in the US and collect popular events that happened nearby.

These events are collected from [eventbrite](https://www.eventbrite.com/) and [ticketmaster](https://www.ticketmaster.com/): Events from ticketmaster are usually large-scale, popular events that could be used to drive price decisions in the future; Events from eventbrite are usually local events that could be used to provide useful information for the coming tourists.

With events in ticketmaster, we can know in advance whether there will be huge events happening in the future and adjust our prices. While with events in eventbrite, we can recommend them to our current guests who are tourists and new to the cities.

# How it works

For each city, we
1. Find the geo coordinate using [positionstack api](https://positionstack.com/documentation);
2. Use the geo coordinate to collect future events nearby from [ticketmaster api](https://developer.ticketmaster.com/);
3. Collect local events from eventbrite in a given date range for current guests;

Finally, we store the events in 2 separate Mongo DB collections, in the real world, we can make this a regular procedure every week to update our event database.
