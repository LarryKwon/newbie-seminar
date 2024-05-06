const express = require('express');
const app = express();
const port = 8080;

/**
 *
 * prisma로 작업하시는 거는 전반적으로 문제될 게 없어서 리뷰는 따로 안 남겼고요.
 * 아랫 부분 정도만 추가로 생각해보시면 좋을 거 같아 남깁니다.
 * 1. deposit은 누구나 할 수 있습니다.
 * 생각해보면 제가 남에게 송금할 때, 다른 사람 계좌에 마음대로 보낼 수 있잖아요? 지금은 deposit을 자신만 할 수 있다고 강력하게 걸어놨는데, 이게 풀리면 어떻게 될까요?
 * 2. deposit이 누구나 가능하다고 하면 다음과 같은 일이 일어날 수 있습니다.
 *
 * 상황 A.
 * 현재 4000원임
 * A가 3000원을 출금하려고 함
 * B는 2000원을 입금함.
 * 모든 일이 일어나고 보니 잔고가 3000원이 아님
 *
 * A는 해당 계좌에 lock을 걸어서 해결할 수 있습니다. lock과 transaction을 걺으로써, 해당 A와 B의 순서를 sequential하게 맞춰주면 이런 일이 발생하지 않습니다.
 */

const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();

app.use(express.json());

app.post('/account/:account_no/withdraw', async (req, res) => {
  const { account_no } = req.params;
  const { firstName, lastName, amount } = req.body;

  const accNumber = parseInt(account_no);

  if (!amount || amount <= 0) {
      return res.status(400).json({ error: "Invalid amount specified" });
  }

  try {
      const account = await prisma.account.findUnique({
          where: { accNumber: accNumber }
      });

      const balance = await prisma.account.findMany({
        where: {
          accNumber: accNumber
        },
        select: {
          balance: true
        }
      });

      const bal = balance.map((e) => (
        e.balance
      ));

      const firstname = await prisma.owns.findMany({
        where: {
          accNumber: accNumber
        },
        select: {
          Customer: {
            select: {
              firstName: true
            }
          }
        }
      });

      const first = firstname.map(e => (
        e.Customer.firstName
      ));

      const lastname = await prisma.owns.findMany({
        where: {
          accNumber: accNumber
        },
        select: {
          Customer: {
            select: {
              lastName: true
            }
          }
        }
      });

      const last = lastname.map(e => (
        e.Customer.lastName
      ));


      if (!account) {
          return res.status(404).json({ error: "Account not found" });
      }

      if ((first != firstName) || (last != lastName)) {
          return res.status(403).json({ error: "Access denied: You do not own this account" });
      }

      if ((bal < amount) || (bal < 0)) {
          return res.status(400).json({ error: "Insufficient balance" });
      }

      const total = parseInt(bal) - amount;
      
    
      const final = total.toString;

      const updatedAccount = await prisma.account.update({
          where: { accNumber: accNumber },
          data: { balance: (total).toString() }
      });

      res.status(200).json({ balance: updatedAccount.balance });
  } catch (error) {
      console.error(error);
      res.status(500).json({ error: "Internal server error" });
  }
});


app.post('/account/:account_no/deposit', async (req, res) => {
  const { account_no } = req.params;
  const { firstName, lastName, amount } = req.body;

  const accNumber = parseInt(account_no);

  if (!amount || amount <= 0) {
      return res.status(400).json({ error: "Invalid amount specified" });
  }

  try {
      const account = await prisma.account.findUnique({
          where: { accNumber: accNumber }
      });

      const balance = await prisma.account.findMany({
        where: {
          accNumber: accNumber
        },
        select: {
          balance: true
        }
      });

      const bal = balance.map((e) => (
        e.balance
      ));

      const firstname = await prisma.owns.findMany({
        where: {
          accNumber: accNumber
        },
        select: {
          Customer: {
            select: {
              firstName: true
            }
          }
        }
      });

      const first = firstname.map(e => (
        e.Customer.firstName
      ));

      const lastname = await prisma.owns.findMany({
        where: {
          accNumber: accNumber
        },
        select: {
          Customer: {
            select: {
              lastName: true
            }
          }
        }
      });

      const last = lastname.map(e => (
        e.Customer.lastName
      ));


      if (!account) {
          return res.status(404).json({ error: "Account not found" });
      }

      if ((first != firstName) || (last != lastName)) {
          return res.status(403).json({ error: "Access denied: You do not own this account" });
      }

      const total = parseInt(bal) + amount;
    
      const updatedAccount = await prisma.account.update({
          where: { accNumber: accNumber },
          data: { balance: total.toString() }
      });

      res.status(200).json({ balance: updatedAccount.balance });
  } catch (error) {
      console.error(error);
      res.status(500).json({ error: "Internal server error" });
  }
});




app.get('/', (req, res) => {
  res.send('chacha Assignment');
});

app.listen(port, () => {
  console.log(`서버 실행 : http://localhost:${port}`);
});