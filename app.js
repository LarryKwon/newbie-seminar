const express = require('express');
const app = express();
const port = 3000;
const {PrismaClient} = require('@prisma/client');
const prisma = new PrismaClient();
const axios = require('axios');
const fs = require('node:fs');
const lodash = require('lodash');

// For powershell: $env:DATABASE_URL="mysql://admin:tnfqkrtm@sparcs-mysql.cpghnwiewsdn.ap-northeast-2.rds.amazonaws.com:3306/teddybear"

app.get('/status', async (req, res) => {
    res.status(200).json({ isOnline: true });
});

const ProblemList = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 17, 18];

app.get('/checker', async (req, res) => {
    rtval = {};
    for (let i = 0; i < ProblemList.length; i++) {
        try {
            const response = await axios.get(`http://localhost:3000/problems/${ProblemList[i]}`);
            const result = response.data;
            const answer = JSON.parse(fs.readFileSync(`Problem${ProblemList[i]}.json`,'utf-8'));
            lodash.isEqual(result, answer) ? rtval[ProblemList[i]] = 'correct' : rtval[ProblemList[i]] = 'incorrect';
        } catch (error) {
            if (error.response) {
                if (error.response.status == 404) {
                    rtval[ProblemList[i]] = 'notimplemented';
                } else {
                    console.log(error.response.status);
                    rtval[ProblemList[i]] = 'error';
                }
            } else {
                console.log(error);
                rtval[ProblemList[i]] = 'error';
            }
        }
    }
    return res.status(200).json(rtval);
});

//sample endpoint
app.get('/problems/0', async (req, res) => {
    try {
        const result = await prisma.branch.findMany();
        return res.status(200).json(result);
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/1', async (req, res) => {
    try {
        const result = await prisma.customer.findMany({
            where: {
                AND: {
                    income: { gt: 50000, },
                    income: { lt: 60000, },
                }
            },
            orderBy: [
                { income: 'desc', },
                { lastName: 'asc', },
                { firstName: 'asc', },
            ],
            select: {
                firstName: true,
                lastName: true,
                income: true
            },
            take: 10
        });
        return res.status(200).json(result);
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/2', async (req, res) => {
    try {
        const London = await prisma.branch.findFirst({
            where: {
                branchName: 'London',
            },
            select: {
                branchNumber: true,
                managerSIN: true,
            },
        });
        const M_London = await prisma.employee.findFirst({
            where: {
                branchNumber: London.branchNumber,
                sin: London.managerSIN,
            },
            select: {
                branchNumber: true,
                sin: true,
                salary: true,
            },
        });
        const E_London = await prisma.employee.findMany({
            where: {
                AND: {
                    branchNumber: London.branchNumber,
                    NOT: {
                        sin: London.managerSIN,
                    },
                },
            },
            select: {
                sin: true,
                salary: true,
            },
        });
        const Berlin = await prisma.branch.findFirst({
            where: {
                branchName: 'Berlin',
            },
            select: {
                branchNumber: true,
                managerSIN: true,
            },
        });
        const M_Berlin = await prisma.employee.findFirst({
            where: {
                branchNumber: Berlin.branchNumber,
                sin: Berlin.managerSIN,
            },
            select: {
                branchNumber: true,
                sin: true,
                salary: true,
            },
        });
        const E_Berlin = await prisma.employee.findMany({
            where: {
                AND: {
                    branchNumber: Berlin.branchNumber,
                    NOT: {
                        sin: Berlin.managerSIN,
                    },
                },
            },
            select: {
                sin: true,
                salary: true,
            },
        });
        var result = [];
        for (const element of E_London) {
            result.push({
                ...element,
                branchName: 'London',
                'Salary Diff': (M_London.salary - element.salary).toString(),
            });
        }
        for (const element of E_Berlin) {
            result.push({
                ...element,
                branchName: 'Berlin',
                'Salary Diff': (M_Berlin.salary - element.salary).toString(),
            });
        }
        result.sort(function (a, b) {
            return parseInt(b['Salary Diff']) - parseInt(a['Salary Diff']);
        });
        return res.status(200).json(result.slice(0, 10));
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/3', async (req, res) => {
    try {
        const C = await prisma.customer.findFirst({
            where: {
                lastName: 'Butler',
            },
            orderBy: [
                { income: 'desc' },
            ],
            select: {
                income: true,
            },
        });
        const result = await prisma.customer.findMany({
            where: {
                income: {
                    gt: C.income*2,
                },
            },
            orderBy: [
                { lastName: 'asc' },
                { firstName: 'asc' },
            ],
            select: {
                firstName: true,
                lastName: true,
                income: true,
            },
            take: 10,
        })
        return res.status(200).json(result);
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/4', async (req, res) => {
    try {
        const q = await prisma.customer.findMany({
            where: {
                income: { gt: 80000 },
            },
            include: {
                Owns: {
                    include: {
                        Account: {
                            include: {
                                Branch: true,
                            },
                        },
                    },
                },
            },
            orderBy: [
                { customerID: 'asc' },
            ]
        });
        var result = [];
        for (const element of q) {
            var flag_a = false;
            var flag_b = false;
            for (const elem of element.Owns) {
                if (elem.Account.Branch.branchName == 'London') {
                    flag_a = true;
                }
                if (elem.Account.Branch.branchName == 'Latveria') {
                    flag_b = true;
                }
            }
            if (!(flag_a && flag_b)) { continue; }
            var tmpResult = [];
            for (const elem of element.Owns) {
                tmpResult.push({
                    customerID: element.customerID,
                    income: element.income,
                    accNumber: elem.accNumber,
                    branchNumber: elem.Account.branchNumber,
                });
            }
            tmpResult.sort(function (a, b) {
                return a.accNumber - b.accNumber;
            });
            for (const elem of tmpResult) {
                result.push(elem);
            }
        }
        return res.status(200).json(result.slice(0, 10));
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/5', async (req, res) => {
    try {
        const q = await prisma.customer.findMany({
            include: {
                Owns: {
                    include: {
                        Account: true,
                    },
                },
            },
            orderBy: [
                { customerID: 'asc' },
            ],
        });
        var result = [];
        for (const element of q) {
            var flag_a = false;
            var flag_b = false;
            for (const elem of element.Owns) {
                if (elem.Account.type == 'BUS') {
                    flag_a = true;
                }
                if (elem.Account.type == 'SAV') {
                    flag_b = true;
                }
            }
            if (!(flag_a || flag_b)) { continue; }
            var tmpResult = [];
            for (const elem of element.Owns) {
                if (!(elem.Account.type == 'BUS' || elem.Account.type == 'SAV')) {
                    continue;
                }
                tmpResult.push({
                    customerID: element.customerID,
                    type: elem.Account.type,
                    accNumber: elem.accNumber,
                    balance: elem.Account.balance,
                });
            }
            tmpResult.sort(function (a, b) {
                return a.accNumber - b.accNumber;
            });
            tmpResult.sort(function (a, b) {
                if (a.type == b.type) return 0;
                return a.type == 'BUS' ? -1 : 1;
            });
            for (const elem of tmpResult) {
                result.push(elem);
            }
        }
        return res.status(200).json(result.slice(0, 10));
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/6', async (req, res) => {
    try {
        const E = await prisma.employee.findFirst({
            where: {
                AND: {
                    firstName: 'Phillip',
                    lastName: 'Edwards',
                },
            },
        });
        const B = await prisma.branch.findFirst({
            where: {
                managerSIN: E.sin,
            },
        });
        const q = await prisma.account.findMany({
            include: {
                Branch: {
                    select: {
                        branchName: true,
                    }
                }
            },
            where: {
                branchNumber: B.branchNumber,
            },
            orderBy: { accNumber: 'asc' },
        });
        var result = [];
        for (const element of q) {
            if (parseFloat(element.balance) < 100000) continue;
            result.push({
                branchName: element.Branch.branchName,
                accNumber: element.accNumber,
                balance: element.balance,
            });
        }
        return res.status(200).json(result.slice(0, 10));
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/7', async (req, res) => {
    try {
        const q = await prisma.customer.findMany({
            include: {
                Owns: {
                    include: {
                        Account: {
                            include: {
                                Branch: {
                                    select: {
                                        branchName: true,
                                    },
                                },
                                Owns: {
                                    include: {
                                        Customer: {
                                            include: {
                                                Owns: {
                                                    include: {
                                                        Account: {
                                                            include: {
                                                                Branch: {
                                                                    select: {
                                                                        branchName: true,
                                                                    },
                                                                },
                                                            },
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
            orderBy: { customerID: 'asc' },
        });
        var result = [];
        for (const element of q) {
            var flag = false;
            for (const elem of element.Owns) {
                if (elem.Account.Branch.branchName == 'New York') { flag = true; }
            }
            if (!flag) { continue; }
            flag = false;
            for (const elem of element.Owns) {
                for (const ele of elem.Account.Owns) {
                    for (const el of ele.Customer.Owns) {
                        if (el.Account.Branch.branchName == 'London') { flag = true; }
                    }
                }
            }
            if (flag) { continue; }
            result.push({
                customerID: element.customerID,
            });
        }
        return res.status(200).json(result.slice(0, 10));
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/8', async (req, res) => {
    try {
        const q = await prisma.employee.findMany({
            where: {
                salary: { gt: 50000 },
            },
            select: {
                sin: true,
                firstName: true,
                lastName: true,
                salary: true,
                Branch_Branch_managerSINToEmployee: true,
            },
            orderBy: [
                { firstName: 'asc' }
            ],
        });
        var result = [];
        for (const element of q) {
            if (element.Branch_Branch_managerSINToEmployee.length != 0) {
                result.push({
                    sin: element.sin,
                    firstName: element.firstName,
                    lastName: element.lastName,
                    salary: element.salary,
                    branchName: element.Branch_Branch_managerSINToEmployee[0].branchName,
                });
            }
        }
        result.sort(function (a, b) {
            return a.branchName > b.branchName ? -1 : 1;
        });
        for (const element of q) {
            if (element.Branch_Branch_managerSINToEmployee.length == 0) {
                result.push({
                    sin: element.sin,
                    firstName: element.firstName,
                    lastName: element.lastName,
                    salary: element.salary,
                    branchName: null,
                });
            }
        }
        return res.status(200).json(result.slice(0, 10));
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/9', async (req, res) => {
    try {
        // Same as Pb. 8 since join operator is not used in Pb. 8
        return res.status(200).json((await axios.get(`http://localhost:3000/problems/8`)).data);
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/10', async (req, res) => {
    try {
        const HM = await prisma.customer.findFirst({
            include: {
                Owns: {
                    include: {
                        Account: true,
                    },
                },
            },
            where: {
                firstName: 'Helen',
                lastName: 'Morgan',
            },
        });
        var HM_b = [];
        for (const element of HM.Owns) {
            HM_b.push(element.Account.branchNumber);
        }
        const q = await prisma.customer.findMany({
            where: {
                income: { gt: 5000 },
            },
            include: {
                Owns: {
                    include: {
                        Account: true,
                    },
                },
            },
            orderBy: { income: 'desc' },
        });
        var result = [];
        for (const element of q) {
            var flag_o = false;
            for (const elem of HM_b) {
                var flag = false;
                for (const ele of element.Owns) {
                    if (ele.Account.branchNumber == elem) { flag = true; }
                }
                if (!flag) { flag_o = true; break; }
            }
            if (flag_o) { continue; }
            result.push({
                customerID: element.customerID,
                firstName: element.firstName,
                lastName: element.lastName,
                income: element.income,
            });
        }
        return res.status(200).json(result.slice(0, 10));
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/11', async (req, res) => {
    try {
        const B = await prisma.branch.findFirst({
            where: { branchName: 'Berlin' },
        });
        const q = await prisma.employee.findMany({
            select: {
                sin: true,
                firstName: true,
                lastName: true,
                salary: true,
            },
            orderBy: { salary: 'asc' },
            where: {
                branchNumber: B.branchNumber,
            },
        });
        var result = [];
        for (const element of q) {
            if (element.salary == q[0].salary) {
                result.push(element);
            }
        }
        return res.status(200).json(result.slice(0, 10));
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/14', async (req, res) => {
    try {
        const M = await prisma.branch.findFirst({
            where: { branchName: 'Moscow' },
        });
        const result = (await prisma.employee.aggregate({
            where: {
                branchNumber: M.branchNumber,
            },
            _sum: {
                salary: true,
            },
        }))._sum.salary;
        return res.status(200).json([{"sum of employees salaries": result.toString()}]);
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/15', async (req, res) => {
    try {
        const q = await prisma.customer.findMany({
            select: {
                customerID: true,
                firstName: true,
                lastName: true,
                Owns: {
                    include: {
                        Account: {
                            include: {
                                Branch: {
                                    select: {
                                        branchName: true
                                    },
                                },
                            },
                        },
                    },
                },
            },
            orderBy: [
                { lastName: 'asc' },
                { firstName: 'asc' },
            ],
        });
        var result = [];
        for (const element of q) {
            var s = new Set();
            for (const elem of element.Owns) {
                s.add(elem.Account.Branch.branchName);
            }
            if (s.size == 4) {
                result.push({
                    customerID: element.customerID,
                    firstName: element.firstName,
                    lastName: element.lastName,
                });
            }
        }
        return res.status(200).json(result.slice(0, 10));
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.listen(port, () => {
    console.log('Starting Server...');
});