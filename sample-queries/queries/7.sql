SELECT region, count(distinct salary), sum(DISTINCT salary)
FROM employee
GROUP BY region
