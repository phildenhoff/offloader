import {
  type Executor
} from "../../src/index"

export const EmailOnSignupExecutor: Executor = () => {
  return {
    name: "EmailOnSignup",
    execute: ({userId}: {userId: number}) => {
      console.log(`'Emailed' user ${userId}`);
    }
  }
}
