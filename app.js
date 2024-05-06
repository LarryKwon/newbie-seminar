const express = require('express');
const app = express();
const port = 3000;
const {PrismaClient} = require('@prisma/client');
const prisma = new PrismaClient();
const axios = require('axios');
const fs = require('node:fs');
const lodash = require('lodash');

app.get('/status', async (req, res) => {
    res.status(200).json({ isOnline: true });
});

/**
 * 전반적으로 코드 가독성이... 조금 떨어져서요.. flag 같은 것도 보이고, s,o 이런 걸 쓰셔서 잘 이해가 안 되는 부분이 있네요
 * 같이 고려하셔서 코드 짜시면 좀 더 좋을 거 같고, 혹시 시스템 쪽 코드 주로 짜셨나요...? 변수 이름이 시스템 코드 보는 거 같아 여쭤봅니다.
 * 그 다음에 js에서 var는 잘 안 쓰시는게 좋습니다.
 * 지금이야 개별적인 함수니까 상관이 없는데 let,var,const는 재선이 되고 안 되고의 문제를 넘어서 스코프 문제까지 이어져서요. 한 번 찾아보시면 좋을 것 같습니다. (호이스팅 개념을 찾아보시면 좋을 거 같아요!)
 * https://www.freecodecamp.org/korean/news/var-let-constyi-caijeomeun/
 *
 * @type {number[]}
 */

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

/**
 * 비즈니스 로직에서 이런 경우로 짜시면 DB Select가 동기적으로 일어나서 좀 느리고요.
 * 좋은 방법은 비동기로 두 개를 한 꺼번에 들고오는 겁니다.
 * await Promise.all([london, latveria]) 이런 식으로 Promise.all을 쓰면 됩니다.
 * const london, latveria = await Promise.all([
 * prisma.owns.findMany({}), prisma.owns.findmany({})])
 * 아마 이런 식으로 짜면 되는 걸로 아는데, 갑자기 쓰려니까 잘 기억이 안 나네요.
 * otl 서버(nest버전) 코드에 이런 식의 접근이 많이 있으니 참고해보세요!
 */

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


/**
 * 이거 이렇게 짜시면..... 안 됩니다.
 * 결과는 맞을 수 있는데, 지금 join이 너무 많아요. 지금이야 데이터셋이 작아서 상관없는데, 이게 지금 몇 중 join인지..
 * 실전에서는 보통 3중 join 넘어가면, 나눠서 들고오던가 subquery로 해결하던가 합니다.
 * 혹시 이렇게 한 이유가 따로 있을까요?
 *
 */
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

app.get('/problems/17', async (req, res) => {
    try {
        const q = await prisma.customer.findMany({
            where: {
                lastName: {
                    startsWith: 'S',
                    contains: 'e',
                },
            },
            include: {
                Owns: {
                    include: {
                        Account: true,
                    },
                },
            },
        });
        var result = [];
        for (const element of q) {
            if (element.Owns.length < 3) { continue; }
            var s = 0;
            var c = 0;
            for (const elem of element.Owns) {
                s += parseFloat(elem.Account.balance);
                c += 1;
            }
            result.push({
                customerID: element.customerID,
                firstName: element.firstName,
                lastName: element.lastName,
                income: element.income,
                'average account balance': s / c,
            });
        }
        return res.status(200).json(result);
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/18', async (req, res) => {
    try {
        const B = await prisma.branch.findFirst({
            where: {
                branchName: 'Berlin',
            },
        });
        const q = await prisma.account.findMany({
            where: {
                branchNumber: B.branchNumber,
            },
            select: {
                accNumber: true,
                balance: true,
                Transactions: true,
            },
        });
        var result = [];
        for (const element of q) {
            if (element.Transactions.length < 10) { continue; }
            var s = 0;
            for (const elem of element.Transactions) {
                s += parseFloat(elem.amount);
            }
            result.push({
                accNumber: element.accNumber,
                balance: element.balance,
                'sum of transaction amounts': s,
            });
        }
        result.sort(function (a, b) {
            return a['sum of transaction amounts'] - b['sum of transaction amounts'];
        });
        return res.status(200).json(result.slice(0, 10));
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.post('/employee/join', async (req, res) => {
    const body = req.body;
    const newEmployee = await prisma.employee.create({
        data: {
            sin: parseInt(body.sin),
            firstName: body.firstName,
            lastName: body.lastName,
            salary: parseInt(body.salary),
            branchNumber: parseInt(body.branchNumber),
        },
    });
    return res.status(200).json(newEmployee);
});

app.post('/employee/leave', async (req, res) => {
    const body = req.body;
    const oldEmployee = await prisma.employee.delete({
        where: {
            sin: parseInt(body.sin),
        },
    });
    return res.status(200).json(oldEmployee);
});

/**
 * 출금/입금 시 자신의 계좌에만 할 수 있다고 했었는데, 해당 조건이 안 보여서요. 실전에서는 문제 조건 꼼꼼히 잘 보시고, 로직 짜주시면 좋을 거 같아요!
 * 누군가가 코드를 짰는데, 문제 조건을 잘못 구현한건 상관이 없는데 문제 조건을 아예 구현하지 않은 것까지 서로 신경써야하면 서로 간의 신뢰가 떨어지는 상황으로 가지 않을까요??
 */

/**
 * 그 다음에 increment, decrement 함수 사용하셨는데, 한 번 쿼리 직접 찍어보셔도 좋을 거 같아요
 * prisma가 실행시키는 쿼리는 prisma.$on 이런식으로 할 수 있어서요. prisma  query loggin으로 검색하셔서 찾아보시면 될 거 같고,
 * increment가 실제로 쿼리에서 lock이랑 함께 단일 update로 나가는지 아니면 select 후 1더한 값을 update로 나가는지 등등 쿼리가 어떻게 나가는지 확인하고 쓸지 말지 고민해보시면 좋을 거 같아요!
 * 쓰면 왜 쓰는게 좋고, 어떨 때 쓰는지 등등
 */
app.post('/account/:account_no/deposit', async (req, res) => {
    const body = req.body;
    const account_no = parseInt(req.params.account_no);
    const updateAccount = await prisma.account.update({
        where: {
            accNumber: account_no,
        },
        data: {
            balance: {
                increment: parseInt(body.amount),
            },
        },
    });
    return res.status(200).json(updateAccount);
});

app.post('/account/:account_no/withdraw', async (req, res) => {
    const body = req.body;
    const account_no = parseInt(req.params.account_no);
    const updateAccount = await prisma.account.update({
        where: {
            accNumber: account_no,
        },
        data: {
            balance: {
                decrement: parseInt(body.amount),
            },
        },
    });
    return res.status(200).json(updateAccount);
});

app.listen(port, () => {
    console.log('Starting Server...');
});