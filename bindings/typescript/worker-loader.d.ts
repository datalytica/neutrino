// typings/custom.d.ts
declare module "worker-loader?name=worker.js!*" {
    class NeutrinoPWorker extends Worker {
      constructor();
    }
  
    export = NeutrinoWorker;
  }