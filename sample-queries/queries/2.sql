SELECT count(*) as num_of_employees
FROM employee INNER JOIN department ON employee.department_id = department.department_id
GROUP BY department.department_id
