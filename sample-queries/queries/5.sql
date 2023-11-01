SELECT count(*) as num_of_employees
FROM employee LEFT JOIN department ON employee.id = department.id
WHERE department.id IS NOT NULL
