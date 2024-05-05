const express = require('express')
const app = express();
const port=3000;
const bcrypt = require('bcryptjs')
const jwt = require('jsonwebtoken')
const {PrismaClient} = require('@prisma/client')
const prisma = new PrismaClient()
const fs=require('node:fs');
const lodash=require('lodash');
const axios=require('axios');
const { orderBy, includes } = require('lodash');

const ProblemList=[1,2,3,4,5,6,7,8,9,10,11,14,15,17,18];



app.listen(port, () => {
  console.log('Starting Server...');
});

app.get('/status', async (req,res) => {
  return res.status(201).json({ server:'running' });
})

app.get('/checker', async (req, res) => {
  // return res.status.json({implemented:'false'})
  let results={};
  for(let j=0;j<ProblemList.length;j++){
    let i=ProblemList[j];
    try{
      // 결과 받기
      const response = await axios.get(`http://localhost:3000/problems/${i}`);
      const result = response.data;
      const answer = JSON.parse(fs.readFileSync(`problem${i}.json`,'utf-8'));
      results[`problems${i}`]=(result.toString() == answer.toString() ? 'Correct' : 'Incorrect');
    } catch (e) {
      if(e.response){
        if(e.response.status == 404){
          results[`problems${i}`]='notImplemented';
        } else{
          results[`problems${i}`]='error';
        }
      } else{
        console.log(e);
        results[`problems${i}`]='error';
      }
    }
  }
  return res.status(201).json(results);
})

// example
app.get('/problems/0', async (req, res) => {
  try{
    const result=await prisma.branch.findMany();
    return res.status(201).json(result);
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/1', async (req, res) => {
  try{
    const result=await prisma.customer.findMany({
      select:{
        firstName:true,
        lastName:true,
        income:true
      },
      where:{
        income:{
          gte:50000,
          lte:60000
        }
      },
      take:10,
      orderBy:[
        {income:'desc'},
        {lastName:'asc'},
        {firstName:'asc'}
      ]
      
    });
    return res.status(201).json(result);
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/2', async (req, res) => {
  try{
    let result=[];
    const London=await prisma.branch.findFirst({
      where:{
        branchName:'London'
      }
    });
    const Berlin=await prisma.branch.findFirst({
      where:{
        branchName:'Berlin'
      }
    });
    const mLondon=await prisma.employee.findFirst({
      where:{
        sin:London.managerSIN
      }
    });
    const mBerlin=await prisma.employee.findFirst({
      where:{
        sin:Berlin.managerSIN
      }
    });
    const eLondon=await prisma.employee.findMany({
      where:{
        branchNumber:London.branchNumber
      },
      select:{
        sin:true,
        salary:true
      }
    });
    const eBerlin=await prisma.employee.findMany({
      where:{
        branchNumber:Berlin.branchNumber
      },
      select:{
        sin:true,
        salary:true
      }
    });
    for(const elem of eLondon){
      result.push({
        ...elem,
        branchName: 'London',
        'Salary Diff': (mLondon.salary-elem.salary).toString()
      });
    }
    for(const elem of eBerlin){
      result.push({
        ...elem,
        branchName: 'Berlin',
        'Salary Diff': (mBerlin.salary-elem.salary).toString()
      });
    }
    result.sort((a,b) => (parseInt(b['Salary Diff'])-parseInt(a['Salary Diff'])));
    return res.status(201).json(result.slice(0,10));
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/3', async (req, res) => {
  try{
    const Butler=await prisma.customer.findMany({
      take:1,
      where:{
        lastName:'Butler'
      },
      select:{
        income:true
      },
      orderBy:{
        income:'desc'
      }
    });
    const result=await prisma.customer.findMany({
      take:10,
      where:{
        income:{
          gte:2*Butler[0].income
        }
      },
      select:{
        firstName:true,
        lastName:true,
        income:true
      },
      orderBy:[
        {lastName:'asc'},
        {firstName:'asc'}
      ]
    });
    return res.status(201).json(result);
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/4', async (req, res) => {
  try{
    let result=[];
    const customers=await prisma.customer.findMany({
      where:{
        income:{
          gt:80000
        }
      },
      include:{
        Owns:{
          include:{
            Account:{
              include:{
                Branch: true
              }
            }
          }
        }
      }
    })

    for(const elem of customers){
      var f1=false,f2=false;
      for(const own of elem.Owns){
        if(own.Account.Branch.branchName=='London')f1=true;
        if(own.Account.Branch.branchName=='Latveria')f2=true;
      }
      if(f1 && f2){
        for(const own of elem.Owns){
          const acc=own.Account;
          result.push({
            customerId:elem.customerID,
            income:elem.income,
            accNumber:acc.accNumber,
            branchNumber:acc.branchNumber
          });
        }
      }
    }

    result.sort((a,b) => (a.customerId-b.customerId || a.accNumber-b.accNumber))

    return res.status(201).json(result.slice(0,10));
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/5', async (req, res) => {
  try{
    const accs=await prisma.account.findMany({
      where:{
        OR:[
          {type:'BUS'},
          {type:'SAV'}
        ]
      },
      include:{
        Owns:true
      }
    })
    let result=[];
    for(elem of accs){
      result.push({
        customerId:elem.Owns[0].customerID,
        type:elem.type,
        accNumber:elem.accNumber,
        balance:elem.balance
      })
    }
    result.sort((a,b) => (a.customerId-b.customerId || (a.type>b.type?1:-(a.type<b.type) || a.accNumber-b.accNumber)))
    return res.status(201).json(result.slice(0,10));
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/6', async (req, res) => {
  try{
    const pe=await prisma.employee.findFirst({
      where:{
        firstName:'Phillip',
        lastName:'Edwards'
      }
    })
    const b=await prisma.branch.findFirst({
      where:{
        managerSIN:pe.sin
      }
    })
    const accs=await prisma.account.findMany({
      include:{
        Branch:true
      },
      where:{
        branchNumber:b.branchNumber
      }
    });

    let result=[]
    for(const acc of accs){
      if(parseFloat(acc.balance)<100000) continue;
      result.push({
        branchName:acc.Branch.branchName,
        accNumber:acc.accNumber,
        balance:acc.balance
      })
    }
    result.sort((a,b) => (a.accNumber-b.accNumber));
    return res.status(201).json(result.slice(0,10));
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/7', async (req, res) => {
  try{
    let picks=[], bans=[];
    const London=await prisma.branch.findFirst({
      where:{
        branchName:'London'
      }
    })
    const NewYork=await prisma.branch.findFirst({
      where:{
        branchName:'New York'
      }
    })
    const pickA=await prisma.account.findMany({
      include:{
        Owns:true,
      },
      where:{
        branchNumber:NewYork.branchNumber
      }
    })
    const banA=await prisma.account.findMany({
      include:{
        Owns:true,
      },
      where:{
        branchNumber:London.branchNumber
      }
    })
    for(e1 of banA){
      bans.push(e1.Owns.customerID);
    }
    for(e1 of pickA){
      const x=e1.Owns.customerID;
      if((x in picks) || x in bans) continue;
      picks.push(x);
    }
    picks.sort();
    let result=[];
    for(let i=0;i<10;i++){
      result.push({
        customerID:picks[i]
      });
    }
    return res.status(201).json(result);
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/8', async (req, res) => {
  try{
    const es=await prisma.employee.findMany({
      where:{
        salary:{
          gt:50000
        }
      }
    })
    let msin=[];
    const ms=await prisma.branch.findMany();
    for(let elem of ms) msin.push(elem.managerSIN);
    let result=[];
    for(e1 of es){
      if(e1.sin in msin){
        result.push({
          sin:e1.sin,
          firstName:e1.firstName,
          lastName:e1.lastName,
          salary:e1.salary,
          branchName:ms[msin.indexOf(e1.sin)].branchName
        })
      }
      else{
        result.push({
          sin:e1.sin,
          firstName:e1.firstName,
          lastName:e1.lastName,
          salary:e1.salary,
          branchName:null
        })
      }
    }
    result.sort((a,b) => ((a.branchName==null)||-(b.branchName==null) || 
    -a.branchName.localeCompare(b.branchName)||
    a.firstName.localeCompare(b.branchName) )) 
    return res.status(201).json(result.slice(0,10));
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/9', async (req, res) => {
  try{
    // #8서 join 안써서 똑같음
    const es=await prisma.employee.findMany({
      where:{
        salary:{
          gt:50000
        }
      }
    })
    let msin=[];
    const ms=await prisma.branch.findMany();
    for(let elem of ms) msin.push(elem.managerSIN);
    let result=[];
    for(e1 of es){
      if(e1.sin in msin){
        result.push({
          sin:e1.sin,
          firstName:e1.firstName,
          lastName:e1.lastName,
          salary:e1.salary,
          branchName:ms[msin.indexOf(e1.sin)].branchName
        })
      }
      else{
        result.push({
          sin:e1.sin,
          firstName:e1.firstName,
          lastName:e1.lastName,
          salary:e1.salary,
          branchName:null
        })
      }
    }
    result.sort((a,b) => ((a.branchName==null)||-(b.branchName==null) || 
    -a.branchName.localeCompare(b.branchName)||
    a.firstName.localeCompare(b.branchName) )) 
    return res.status(201).json(result.slice(0,10));
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/10', async (req, res) => {
  try{
    const hm=await prisma.customer.findFirst({
      where:{
        firstName:'Helen',
        lastName:'Morgan'
      },
      include:{
        Owns:{
          include:{
            Account:true
          }
        }
      }
    })
    const cs=await prisma.customer.findMany({
      where:{
        income:{
          gt:5000
        }
      },
      include:{
        Owns:{
          include:{
            Account:true
          }
        }
      }
    })
    let hmbn=[],result=[];
    for(const elem of hm.Owns){
      hmbn.push(elem.Account.branchNumber);
    }
    for(const c of cs){
      let f=true;
      for(const x of hmbn){
        let ff=false;
        for(const elem of c.Owns){
          if(elem.Account.branchNumber == x){
            ff=true;
            break;
          }
        }
        if(!ff) f=false;
      }
      if(f){
        result.push({
          customerID:c.customerID,
          firstName:c.firstName,
          lastName:c.lastName,
          income:c.income
        })
      }
    }

    result.sort((a,b) => (b.income-a.income));
    
    return res.status(201).json(result.slice(0,10));
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/11', async (req, res) => {
  try {
    const b=await prisma.branch.findFirst({
      where:{
        branchName:'Berlin'
      }
    });
    const result=await prisma.employee.findMany({
      take:1,
      select:{
        sin:true,
        firstName:true,
        lastName:true,
        salary:true
      },
      where:{
        branchNumber:b.branchNumber
      },
      orderBy:{salary:'asc'}
    });
    return res.status(201).json(result[0]);
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/14', async (req, res) => {
  try{
    const m=await prisma.branch.findFirst({
      where:{
        branchName:'Moscow'
      }
    })
    const es=await prisma.employee.findMany({
      where:{
        branchNumber:m.branchNumber
      },
      select:{
        salary:true
      }
    })
    let s=0;
    for(const elem in es){
      s += elem.salary;
    }
    return res.status(201).json({"sums of employees salaries":toString(s)});
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/15', async (req, res) => {
  try{
    const cs=await prisma.customer.findMany({
      include:{
        Owns:true
      },
      orderBy:[
        {lastName:'asc'},
        {firstName:'asc'}
      ]
    })
    let result=[];
    const as=await prisma.account.findMany();
    for(const c of cs){
      var s=new Set();
      for(const elem of as){
        for(const o of c.Owns){
          if(o.accNumber==elem.accNumber){
            s.add(elem.branchNumber);
          }
        }
      }
      if(s.size==4)result.push({
        customerID:c.customerID,
        firstName:c.firstName,
        lastName:c.lastName
      });
    }
    return res.status(201).json(result.slice(0,10));
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/17', async (req, res) => {
  try{
    const cs = await prisma.customer.findMany({
      where: {
          lastName: {
              startsWith: 'S',
              contains: 'e',
          }
      },
      include: {
          Owns: {
              include: {
                  Account: true,
              }
          }
      }
  });
    let result=[];
    for(const elem of cs){
      if(elem.Owns.length<3) continue;
      let sum=0, cnt=0;
      for(const o of elem.Owns){
        cnt+=1;
        sum += parseFloat(o.Account.balance);
      }
      result.push({
        customerID: elem.customerID,
        firstName: elem.firstName,
        lastName: elem.lastName,
        income: elem.income,
        'average account balance': sum/cnt,
      })
    }
    result.sort((a,b) => a.customerID-b.customerID);
    return res.status(201).json(result.slice(0,10));
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.get('/problems/18', async (req, res) => {
  try{
    const b=await prisma.branch.findFirst({
      where:{
        branchName:'Berlin'
      }
    })
    const as = await prisma.account.findMany({
      where:{
        branchNumber:b.branchNumber
      }
    })
    let result=[];
    for(const a in as){
      const ts = await prisma.transactions.findMany({
        where:{
          accNumber:a.accNumber
        }
      });
      if(ts.length<10) continue;
      let s=0.0;
      for(elem of ts){
        s += elem.amount;
      }
      result.push({
        accNumber: a.accNumber,
        balance: a.balance,
        'sum of transaction amounts': s,
      });
    }
    result.sort((a,b) => (a['sum of transaction amounts']-b['sum of transaction amounts']));
    return res.status(201).json(result.slice(0,10));
  } catch (e){
    console.log(e);
    return res.status(500).json();
  }

})

app.post('/employee/join', async (req, res) => {
  const body = req.body;
  const newE = await prisma.employee.create({
      data: {
          sin: parseInt(body.sin),
          firstName: body.firstName,
          lastName: body.lastName,
          salary: parseInt(body.salary),
          branchNumber: parseInt(body.branchNumber)
      }
  });
  return res.status(200).json(newE);
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