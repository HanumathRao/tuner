SELECT count(*) as num_of_employees
FROM employee INNER JOIN department ON employee.id = department.id
GROUP BY department.id
