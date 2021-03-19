SELECT DISTINCT Employees.eid
FROM Employees, Certified, Schedule
WHERE Employees.eid = Certified.eid, Certified.aid = Schedule.aid
