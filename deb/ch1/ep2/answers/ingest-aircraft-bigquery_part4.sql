-- all the distinct manufacturers of aircraft
SELECT DISTINCT manufacturer 
FROM deb.aircraft;


-- unique models of aircraft
SELECT COUNT(DISTINCT model) as Models 
FROM deb.aircraft;

-- oldest model plane(s)?
SELECT * FROM deb.aircraft
WHERE year = (SELECT MIN(year) FROM deb.aircraft);
