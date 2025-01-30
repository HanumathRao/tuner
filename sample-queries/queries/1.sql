SELECT *
FROM employee INNER JOIN department
ON employee.department_id = department.department_id and employee.department_id = 100
