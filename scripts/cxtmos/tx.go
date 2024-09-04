//nolint:unused,staticcheck,wrapcheck,whitespace,ineffassign,nlreturn
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	clientTx "github.com/cosmos/cosmos-sdk/client/tx"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/cosmos/cosmos-sdk/types/tx/signing"
	xauthsigning "github.com/cosmos/cosmos-sdk/x/auth/signing"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc"

	ewmapp "github.com/covalenthq/ewm-types/app"
	ewmparams "github.com/covalenthq/ewm-types/app/params"
	ewmtypes "github.com/covalenthq/ewm-types/x/ewm/types"
)

var encCfg ewmparams.EncodingConfig

func init() {
	encCfg = ewmapp.MakeEncodingConfig()
}

// covenet accounts
func main() {
	privKeyHexes := []string{
		"619b2dd7558ffd6f4f0675527e02beff1a3e7cecf13234d043a5e00a6a575cb6", // alice
		"54b14b20646947d5d9d07f7cd52ba62254a85102cff072b3899ab08ebb81b7d9", // bob
		"2cbaf56ec09a017fcedafcc08146ead957dcbaafdf319c9d36b1df56043fc659", // carol
		"39fe0bdce51e492f8c581f68d5c8f530872f906cc0f22379648bd2238a72e33a", // david
		"d2e5bea6a544ae98c5164b23dbadd4df58be7910deb44d19064dd6d6e6e6df98", // eve
	}

	// Get public, private and account addresses from hex keys
	privateKeys, publicKeys, addresses, err := processPrivateKeys(privKeyHexes)
	if err != nil {
		fmt.Printf("Error processing private keys: %v\n", err)

		return
	}

	// Get the GRPC node connection
	grpcConn, err := getGRPCConnection()
	if err != nil {
		fmt.Printf("Error processing grpc connection: %v\n", err)

		return
	}

	// Get account information like nonce and number
	sequences, numbers, err := queryAccountInfo(grpcConn, addresses)
	if err != nil {
		fmt.Printf("Error processing sequence queries: %v\n", err)
		return
	}

	fmt.Println("GRPC connection status:", grpcConn.GetState())

	for key := 0; key < 5; key++ {
		fmt.Printf("Key Set %d:\n", key+1)
		fmt.Printf("  Private Key: %x\n", privateKeys[key].Bytes())
		fmt.Printf("  Public Key: %x\n", publicKeys[key].Bytes())
		fmt.Printf("  Address: %s\n", addresses[key].String())
		fmt.Printf("  Sequence: %d\n", sequences[key])
		fmt.Printf("  Account Number: %d\n", numbers[key])
		fmt.Println()
	}

	// Send the proof transaction with account data
	err = sendProofTx(privateKeys, publicKeys, addresses, sequences, numbers, encCfg)
	if err != nil {
		log.Error(err)
	}

	// Allow time for tx mine
	time.Sleep(5 * time.Second)

	// Get tx balances
	// getStakeBalances(grpcConn, addresses)
	// getCovenetSysInfo(grpcConn)

	fmt.Println("process completed")

	// Close connection
	err = grpcConn.Close()
	if err != nil {
		log.Error(err)
	}
}

func getCovenetSysInfo(grpcConn *grpc.ClientConn) error {
	// This creates a gRPC client to query the x/covenet service.
	covenetClient := ewmtypes.NewQueryClient(grpcConn)
	params := &ewmtypes.QueryGetSystemInfoRequest{}

	res, err := covenetClient.SystemInfo(context.Background(), params)
	if err != nil {
		return err
	}
	fmt.Printf("System Info: %s\n", res)

	return nil
}

func getGRPCConnection() (*grpc.ClientConn, error) {
	// Create a connection to the gRPC server.
	grpcConn, err := grpc.Dial(
		"localhost:9090",    // Or your gRPC server address.
		grpc.WithInsecure(), // The Cosmos SDK doesn't support any transport security mechanism.
	)
	if err != nil {

		return nil, err
	}

	return grpcConn, nil
}

func processPrivateKeys(privKeyHexes []string) ([]cryptotypes.PrivKey, []cryptotypes.PubKey, []sdk.AccAddress, error) {
	if len(privKeyHexes) != 5 {

		return nil, nil, nil, fmt.Errorf("expected 5 private keys, got %d", len(privKeyHexes))
	}

	var privateKeys []cryptotypes.PrivKey
	var publicKeys []cryptotypes.PubKey
	var addresses []sdk.AccAddress

	// Set the bech32 prefix for your chain
	const (
		Bech32PrefixAccAddr = "cxtmos"
	)

	// Configure the address prefix
	config := sdk.GetConfig()
	config.SetBech32PrefixForAccount(Bech32PrefixAccAddr, Bech32PrefixAccAddr+"pub")

	for _, privKeyHex := range privKeyHexes {
		// Decode the hex string to bytes
		privKeyBytes, err := hex.DecodeString(privKeyHex)
		if err != nil {

			return nil, nil, nil, fmt.Errorf("error decoding private key: %w", err)
		}

		// Create a new PrivKey object
		privKey := &secp256k1.PrivKey{Key: privKeyBytes}

		// Derive public key
		pubKey := privKey.PubKey()

		// Get the address bytes
		addrBytes := sdk.AccAddress(pubKey.Address())

		// Encode with the correct prefix
		bech32Addr, err := bech32.ConvertAndEncode(Bech32PrefixAccAddr, addrBytes)
		if err != nil {

			return nil, nil, nil, fmt.Errorf("error encoding bech32 address: %w", err)
		}

		// If you specifically need a Covenet address type, you might do:
		covenetAddr, err := ewmtypes.CovenetAccAddressFromBech32(bech32Addr)
		if err != nil {

			return nil, nil, nil, fmt.Errorf("error converting to Covenet address: %w", err)
		}

		privateKeys = append(privateKeys, privKey)
		publicKeys = append(publicKeys, pubKey)
		addresses = append(addresses, covenetAddr)
	}

	return privateKeys, publicKeys, addresses, nil
}

func queryAccountInfo(grpcConn *grpc.ClientConn, addresses []sdk.AccAddress) ([]uint64, []uint64, error) {
	// Create a new auth query client
	authClient := authtypes.NewQueryClient(grpcConn)
	sequences := make([]uint64, len(addresses))
	numbers := make([]uint64, len(addresses))

	for index, addr := range addresses {
		// Query the account
		res, err := authClient.Account(
			context.Background(),
			&authtypes.QueryAccountRequest{Address: addr.String()},
		)
		if err != nil {
			// If the account is not found, set sequence to 0
			if err.Error() == "rpc error: code = NotFound desc = account cosmos1... not found: key not found" {
				sequences[index] = 0
				continue
			}

			return nil, nil, fmt.Errorf("failed to query account %s: %w", addr.String(), err)
		}

		// Create a new AccountI interface
		var account authtypes.AccountI

		// Unmarshal the account data
		err = encCfg.InterfaceRegistry.UnpackAny(res.Account, &account)
		if err != nil {

			return nil, nil, fmt.Errorf("failed to unpack account %s: %w", addr.String(), err)
		}

		// Get the sequence number
		sequences[index] = account.GetSequence()
		numbers[index] = account.GetAccountNumber()
	}

	return sequences, numbers, nil
}

func sendProofTx(privateKeys []cryptotypes.PrivKey, publicKeys []cryptotypes.PubKey, addresses []sdk.AccAddress, sequences []uint64, numbers []uint64, encCfg ewmparams.EncodingConfig) error {
	// Choose your codec: Amino or Protobuf. Here, we use Protobuf, given by the
	_ = publicKeys

	// Create a new TxBuilder.
	txBuilder := encCfg.TxConfig.NewTxBuilder()

	proofMsg := ewmtypes.NewMsgCreateProof(addresses[2].String(), 1, "specimen", 20578635, "0x951c58a73f21ba4eea2c69c93fdadd57291eb4dd70576ea350725a3609f44a09", "0xb4ef1b0b10188d36f08e053ae0a81162a258cd57df8590428cfea75f0cbfa45f", "ipfs://bafybeifcznetub6g37t54henvbijgqsyu4o67radj7gq4fx37th4pb3gsy")
	// https://moonscan.io/tx/0xe8c6ee21ccc7588958b91436b346c8a50d2c5a383500b934aac28d7f22166aa9

	err := txBuilder.SetMsgs(proofMsg)
	if err != nil {

		return err
	}

	txBuilder.SetGasLimit(200000)
	// txBuilder.SetFeeAmount(...)
	// txBuilder.SetMemo(...)
	// txBuilder.SetTimeoutHeight(...)

	// Assuming we have a single private key, account number, and sequence
	priv := privateKeys[2]
	accNum := numbers[2]
	accSeq := sequences[2]

	fmt.Println(accNum, accSeq, "account details")

	sigV2 := signing.SignatureV2{
		PubKey: priv.PubKey(),
		Data: &signing.SingleSignatureData{
			SignMode:  encCfg.TxConfig.SignModeHandler().DefaultMode(),
			Signature: nil,
		},
		Sequence: accSeq,
	}

	err = txBuilder.SetSignatures(sigV2)
	if err != nil {

		return err
	}

	// Second round: actual signing
	signerData := xauthsigning.SignerData{
		ChainID:       "covenet",
		AccountNumber: accNum,
		Sequence:      accSeq,
	}
	sigV2, err = clientTx.SignWithPrivKey(
		encCfg.TxConfig.SignModeHandler().DefaultMode(), signerData,
		txBuilder, priv, encCfg.TxConfig, accSeq)
	if err != nil {

		return err
	}

	err = txBuilder.SetSignatures(sigV2)
	if err != nil {

		return err
	}

	// Generated Protobuf-encoded bytes.
	txBytes, err := encCfg.TxConfig.TxEncoder()(txBuilder.GetTx())

	// Generate a JSON string.
	txJSONBytes, err := encCfg.TxConfig.TxJSONEncoder()(txBuilder.GetTx())
	if err != nil {

		return err
	}
	txJSON := string(txJSONBytes)

	fmt.Println("tx JSON:\n", txJSON)

	grpcConn, err := getGRPCConnection()
	if err != nil {
		return err
	}
	// Broadcast the tx via gRPC. We create a new client for the Protobuf Tx
	// service.
	txClient := tx.NewServiceClient(grpcConn)
	// We then call the BroadcastTx method on this client.
	grpcRes, err := txClient.BroadcastTx(
		context.Background(),
		&tx.BroadcastTxRequest{
			Mode:    tx.BroadcastMode_BROADCAST_MODE_SYNC,
			TxBytes: txBytes, // Proto-binary of the signed transaction, see previous step.
		},
	)
	if err != nil {

		return err
	}

	fmt.Println("response code\n", grpcRes.TxResponse.String()) // Should be `0` if the tx is successful

	return nil
}
