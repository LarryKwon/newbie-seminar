import { Router } from 'express';
import { AccountService } from "../services/accountService";

const accountRouter = Router();

const accountService = new AccountService();

accountRouter.post('/account/:account_no/deposit', async (req, res) => {
    try {
        const { accNumber, customerID, amount } = req.body;
        const updatedBalance =
            await accountService.deposit({ accNumber, customerID, amount });
        res.send(updatedBalance);
    } catch (e: any) {
        console.error(e);
        res.status(500).send({ message: e.message });
    }
});

accountRouter.post('/account/:account_no/withdraw', async (req, res) => {
    try {
        const { accNumber, customerID, amount } = req.body;
        const updatedBalance =
            await accountService.withdraw({ accNumber, customerID, amount });
        res.send(updatedBalance);
    } catch (e: any) {
        console.error(e);
        res.status(500).send({ message: e.message });
    }
});

export default accountRouter;
