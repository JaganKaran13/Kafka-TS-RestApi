import { JsonController, Param, Body, Get, Post, Put, Delete, Req, Res } from "routing-controllers";
import { UserModel } from '../models/UserModel';
import { Request, Response } from "express";

@JsonController("/kafka")
export class KafkaController {

    @Post("/")
    async produceMessage(@Req() req: Request, @Res() res: Response, @Body() userDetails: UserModel): Promise<any> {
        try {
            console.log(userDetails);
            return res.sendStatus(200);
        } catch (exception) {
            console.log(exception);
            return res.sendStatus(403);
        }
    }
}
