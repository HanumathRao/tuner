SELECT count(*) as num_of_employees
FROM employee LEFT JOIN department ON employee.id = department.id
WHERE employee.id IS NOT NULL
