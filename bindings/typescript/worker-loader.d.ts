// typings/custom.d.ts
declare module "worker-loader!*" {
    class NeutrinoWorker extends Worker {
      constructor();
    }
  
    export = NeutrinoWorker;
}