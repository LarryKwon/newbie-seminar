1. Select the [first name, last name, income] of customers whose income is within
   [$50,000, $60,000] and order by income (desc), last name (asc), and then first name
   (asc).
2. Select the [SIN, branch name, salary, manager’s salary - salary (that is, the salary of
   the employee’s manager minus salary of the employee)] of all employees in London
   or Berlin, and order by descending (manager’s salary - salary).
3. Select the [first name, last name, income] of customers whose income is at least double the income of every customer whose last name is Butler, and order by last
   name (asc) then first name (asc).
4. Select the [customer ID, income, account number, branch number] of customers with incomes greater than 80,000 who own an account at both London and Latveria branches, and order by customer ID (asc) then account number (asc).
   The result should contain all the account numbers of customers who meet the criteria, even if the account itself is not held at London or Latveria.
   For example, if a customer with an income greater than $80,000 owns accounts in London, Latveria, and New York, the customer meets the criteria, and the New York account must also be in the result.
5. Select the [customer ID, type, account number, balance] of business (type BUS) and
   savings (type SAV) accounts owned by customers who own at least one business
   account or at least one savings account, and order by customer ID (asc), type (asc),
   and then account number (asc).
6. Select the [branch name, account number, balance] of accounts with balances greater
   than $100,000 held at the branch managed by Phillip Edwards, and order by account
   number (asc).
7. Select the [customer ID] of customers that 1) have an account at the New York
   branch, 2) do not own an account at the London branch, 3) do not co-own an account with another customer who owns an account at the London branch. Order the result by customer ID (asc). The result should not contain duplicate customerIDs. Write a query satisfying all three conditions.
8. Select the [SIN, first name, last name, salary, branch name] of employees who earn more than $50,000. If an employee is a manager, show the branch name of his/her branch. Otherwise, insert a NULL value for the branch name (the fifth column).
   Order the result by branch name (desc) then first name (asc). You must use an
   outer join in your solution for this problem.
9. Solve question (8) again without using any join operations. Here, using an implicit
   cross join such as FROM A, B is accepted, but there should be no JOIN operator in
   your query.
10. Select the [customer ID, first name, last name, income] of customers who have incomes greater than $5000 and own accounts in ALL of the branches that Helen Morgan owns accounts in, and order by income (desc).
    For example, if Helen owns accounts in London and Berlin, a customer who owns accounts in London, Berlin,and New York has to be included in the result.
    If a customer owns accounts in London and New York, the customer does not have to be in the result. The result should also contain Helen Morgan.
11. Select the [SIN, first name, last name, salary] of the lowest paid employee (or em-
    ployees) of the Berlin branch, and order by sin (asc).
12. ~~Select the [branch name, the difference of maximum salary and minimum salary
    (salary gap), average salary] of the employees at each branch, and order by branch
    name (asc).~~
13. ~~Select two values: (1) the number of employees working at the New York branch
    and (2) the number of different last names of employees working at the New York
    branch. The result should contain two numbers in a single row. Name the two
    columns countNY and coundDIFF using the AS keyword.~~
14. Select the [sum of the employee salaries] at the Moscow branch. The result should
    contain a single number.
15. Select the [customer ID, first name, last name] of customers who own accounts from
    only four different types of branches, and order by last name (asc) then first name
    (asc).
16. ~~Select the [average income] of customers older than 60 and [average income] of
    customers younger than 26. The result should contain the two numbers in a single
    row. (Hint: you can use MySQL time and date functions here:
    http://www.mysqltutorial.org/mysql-timestampdiff/ )~~
17. Select the [customer ID, first name, last name, income, average account balance] of
    customers who have at least three accounts, and whose last names begin with S and
    contain an e (e.g. Steve), and order by customer ID (asc).
18. Select the [account number, balance, sum of transaction amounts] for accounts in
    the Berlin branch that have at least 10 transactions, and order by transaction sum
    (asc).
19. ~~Select the [branch name, account type, average transaction amount] of each account
    type of a branch for branches that have at least 50 accounts of any type. Order the
    result by branch name (asc) then account type (asc).~~
20. ~~Select the [account type, account number, transaction number, amount of transac-
    tions] of accounts where the average transaction amount is greater than three times
    the (overall) average transaction amount of accounts of that type. For example,
    if the average transaction amount of all business accounts is $2,000 then return
    transactions from business accounts where the average transaction amount for that
    account is greater than $6,000. Order by account type (asc), account number (asc),
    and then transaction number (asc). Note that all transactions of the qualifying
    accounts should be returned even if their amounts are less than the average amount
    of the account type.~~
