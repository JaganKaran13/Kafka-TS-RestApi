// UserModel Class.
export class UserModel {

    name: string;
    age: number;
    fav: string[];
    dob: string;

    constructor(name: string, age: number, fav: string[], dob: string) {
        this.name = name;
        this.age = age;
        this.fav = fav;
        this.dob = dob;
    }

    static USER_SCHEMA = {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "maxLength": 10,
                "minLength": 5,

            },
            "age": {
                "type": "number",
            },
            "fav": {
                "type": "array",
                "items": { "type": "string" }
            },
            "dob": {
                "type": "string",
                "format": "date"
            }
        }
    };

}