SELECT count(*) as num_of_employees
FROM employee LEFT JOIN department ON employee.department_id = department.department_id
WHERE department.department_id IS NOT NULL
