SELECT *
FROM employee INNER JOIN department
ON employee.department_id = department.id and employee.id = 100
