-- Total Bookings
SELECT COUNT(*) AS total_bookings
FROM gold_hotel_booking;


-- Total Guests
SELECT SUM(num_guests) AS total_guests
FROM gold_hotel_booking;


-- Average Booking Value
SELECT AVG(usd_total_amount) AS avg_booking_value
FROM gold_hotel_booking;


-- Total Revenue
SELECT SUM(usd_total_amount) AS total_revenue
FROM gold_hotel_booking;


-- Monthly Revenue (ordered by check-in date)
SELECT check_in_date, usd_total_amount
FROM gold_hotel_booking
ORDER BY check_in_date;


-- Monthly Booking Count
SELECT check_in_date, COUNT(booking_status) AS total_bookings
FROM gold_hotel_booking
GROUP BY check_in_date
ORDER BY check_in_date;


-- Top Cities by Revenue
SELECT hotel_city, SUM(usd_total_amount) AS total_revenue
FROM gold_hotel_booking
WHERE usd_total_amount IS NOT NULL
GROUP BY hotel_city
ORDER BY total_revenue DESC
LIMIT 10;


-- Booking Status Counts
SELECT booking_status, COUNT(*) AS total
FROM gold_hotel_booking
GROUP BY booking_status;


-- Room Type Counts
SELECT room_type, COUNT(*) AS total_bookings
FROM gold_hotel_booking
GROUP BY room_type
ORDER BY total_bookings DESC;
