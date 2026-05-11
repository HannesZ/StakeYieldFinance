import { HardhatUserConfig } from "hardhat/config";
import "@nomicfoundation/hardhat-toolbox";
import "dotenv/config";

const config: HardhatUserConfig = {
  networks: {
    sepolia: {
      url: process.env.SEPOLIA_RPC_URL || "https://sepolia.drpc.org",
      accounts: process.env.DEPLOYER_KEY ? [process.env.DEPLOYER_KEY] : [],
      chainId: 11155111,
    },
    holesky: {
      url: process.env.HOLESKY_RPC_URL || "https://ethereum-holesky-rpc.publicnode.com",
      accounts: process.env.DEPLOYER_KEY ? [process.env.DEPLOYER_KEY] : [],
      chainId: 17000,
    },
    hoodi: {
      url: process.env.HOODI_RPC_URL || "https://rpc.hoodi.ethpandaops.io",
      accounts: process.env.DEPLOYER_KEY ? [process.env.DEPLOYER_KEY] : [],
      chainId: 560048,
    },
  },
  etherscan: {
    apiKey: process.env.ETHERSCAN_API_KEY || "",
  },
  solidity: {
    version: "0.8.27",
    settings: {
      optimizer: {
        enabled: true,
        runs: 200,
      },
      viaIR: true,
      evmVersion: "cancun",
    },
  },
  paths: {
    sources: "./contracts",
    tests: "./test",
    cache: "./cache",
    artifacts: "./artifacts",
  },
};

export default config;
