import { Router } from 'express';
import ProblemService from  "../services/problemService";
import Util from "../util";

const problemRouter = Router();

const problemService = new ProblemService(new Util());

problemRouter.get('/:id', async (req, res) => {
    try {
        const id = Number(req.params.id);
        const result = await problemService.solve(id);
        res.send(result);
    } catch (e) {
        console.error(e);
        res.status(500).send({ message: 'An error occurred.'});
    }
});

export default problemRouter;
