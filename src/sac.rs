//! Stellar Asset Contract (SAC) detection and asset enrichment (issue #1254).
//!
//! A SAC's contract ID is not random: it is
//! `sha256(XDR(HashIdPreimage::ContractId { network_id, Asset(asset) }))`
//! where `network_id = sha256(network_passphrase)`. Every event a SAC emits
//! carries its SEP-11 asset string (`"native"` or `"CODE:ISSUER"`) as the
//! last topic, so an event is from a SAC exactly when deriving the contract
//! ID from that asset string reproduces the emitting contract's ID. A
//! non-SAC contract cannot fake this without a sha256 preimage.

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::str::FromStr;
use stellar_xdr::curr::{
    AccountId, AlphaNum12, AlphaNum4, Asset, AssetCode12, AssetCode4, ContractId,
    ContractIdPreimage, Hash, HashIdPreimage, HashIdPreimageContractId, Limits, ScVal, WriteXdr,
};

pub const PUBNET_PASSPHRASE: &str = "Public Global Stellar Network ; September 2015";
pub const TESTNET_PASSPHRASE: &str = "Test SDF Network ; September 2015";
pub const FUTURENET_PASSPHRASE: &str = "Test SDF Future Network ; October 2022";

/// The underlying classic asset of a SAC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SacAsset {
    /// `"native"` for XLM, otherwise the asset code (e.g. `"USDC"`).
    pub code: String,
    /// Issuer account (`G...`); `None` for the native asset.
    pub issuer: Option<String>,
}

/// Network passphrase for an indexer `chain_id`. `NETWORK_PASSPHRASE`
/// overrides the built-in mapping (needed for standalone/private networks).
pub fn network_passphrase(chain_id: &str) -> String {
    if let Ok(p) = std::env::var("NETWORK_PASSPHRASE") {
        if !p.is_empty() {
            return p;
        }
    }
    match chain_id.to_ascii_lowercase().as_str() {
        "testnet" | "soroban-testnet" => TESTNET_PASSPHRASE,
        "futurenet" | "soroban-futurenet" => FUTURENET_PASSPHRASE,
        _ => PUBNET_PASSPHRASE,
    }
    .to_string()
}

pub fn network_id(passphrase: &str) -> [u8; 32] {
    Sha256::digest(passphrase.as_bytes()).into()
}

fn to_xdr_asset(asset: &SacAsset) -> Option<Asset> {
    let Some(issuer) = &asset.issuer else {
        return (asset.code == "native").then_some(Asset::Native);
    };
    let issuer = AccountId::from_str(issuer).ok()?;
    match asset.code.len() {
        1..=4 => Some(Asset::CreditAlphanum4(AlphaNum4 {
            asset_code: AssetCode4::from_str(&asset.code).ok()?,
            issuer,
        })),
        5..=12 => Some(Asset::CreditAlphanum12(AlphaNum12 {
            asset_code: AssetCode12::from_str(&asset.code).ok()?,
            issuer,
        })),
        _ => None,
    }
}

/// Parse a SEP-11 asset string: `"native"` or `"CODE:ISSUER"`.
pub fn parse_asset(s: &str) -> Option<SacAsset> {
    if s == "native" {
        return Some(SacAsset { code: "native".into(), issuer: None });
    }
    let (code, issuer) = s.split_once(':')?;
    let asset = SacAsset { code: code.to_string(), issuer: Some(issuer.to_string()) };
    // Round-trip through XDR so malformed codes/issuers are rejected here.
    to_xdr_asset(&asset).map(|_| asset)
}

/// The `C...` contract ID of the SAC for `asset` on the given network.
pub fn sac_contract_id(asset: &SacAsset, network_id: &[u8; 32]) -> Option<String> {
    let preimage = HashIdPreimage::ContractId(HashIdPreimageContractId {
        network_id: Hash(*network_id),
        contract_id_preimage: ContractIdPreimage::Asset(to_xdr_asset(asset)?),
    });
    let xdr = preimage.to_xdr(Limits::none()).ok()?;
    let hash: [u8; 32] = Sha256::digest(xdr).into();
    Some(ContractId(Hash(hash)).to_string())
}

/// Return the underlying asset when `contract_id` is the SAC named by the
/// event's trailing asset topic, `None` for every non-SAC event.
pub fn detect(contract_id: &str, topics: &[Value], network_id: &[u8; 32]) -> Option<SacAsset> {
    // SAC events always have at least the event name plus the asset topic.
    if topics.len() < 2 {
        return None;
    }
    let ScVal::String(s) = crate::scval_format::parse_stored(topics.last()?)? else {
        return None;
    };
    let asset = parse_asset(std::str::from_utf8(s.as_slice()).ok()?)?;
    (sac_contract_id(&asset, network_id)? == contract_id).then_some(asset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use stellar_xdr::curr::{ScString, ScSymbol};

    const USDC_ISSUER: &str = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN";
    const USDC_SAC: &str = "CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75";
    const XLM_SAC: &str = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA";

    fn pubnet() -> [u8; 32] {
        network_id(PUBNET_PASSPHRASE)
    }

    fn b64(v: ScVal) -> Value {
        json!(v.to_xdr_base64(Limits::none()).unwrap())
    }

    fn transfer_topics(asset: &str) -> Vec<Value> {
        vec![
            b64(ScVal::Symbol(ScSymbol::try_from("transfer").unwrap())),
            b64(ScVal::Symbol(ScSymbol::try_from("from").unwrap())),
            b64(ScVal::String(ScString(asset.try_into().unwrap()))),
        ]
    }

    #[test]
    fn derives_known_mainnet_sac_ids() {
        let usdc = parse_asset(&format!("USDC:{USDC_ISSUER}")).unwrap();
        assert_eq!(sac_contract_id(&usdc, &pubnet()).unwrap(), USDC_SAC);
        let xlm = parse_asset("native").unwrap();
        assert_eq!(sac_contract_id(&xlm, &pubnet()).unwrap(), XLM_SAC);
    }

    #[test]
    fn detects_sac_event_from_xdr_topics() {
        let asset = detect(USDC_SAC, &transfer_topics(&format!("USDC:{USDC_ISSUER}")), &pubnet()).unwrap();
        assert_eq!(asset.code, "USDC");
        assert_eq!(asset.issuer.as_deref(), Some(USDC_ISSUER));
        let native = detect(XLM_SAC, &transfer_topics("native"), &pubnet()).unwrap();
        assert_eq!(native, SacAsset { code: "native".into(), issuer: None });
    }

    #[test]
    fn detects_sac_event_from_serde_json_topics() {
        let topics = vec![json!({"symbol": "transfer"}), json!({"string": format!("USDC:{USDC_ISSUER}")})];
        assert!(detect(USDC_SAC, &topics, &pubnet()).is_some());
    }

    #[test]
    fn rejects_spoofed_asset_topic_and_other_networks() {
        // A custom contract emitting the USDC asset string is not the SAC.
        let spoof = "CA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUWDA";
        assert!(detect(spoof, &transfer_topics(&format!("USDC:{USDC_ISSUER}")), &pubnet()).is_none());
        let testnet = network_id(TESTNET_PASSPHRASE);
        assert!(detect(USDC_SAC, &transfer_topics(&format!("USDC:{USDC_ISSUER}")), &testnet).is_none());
    }

    #[test]
    fn non_sac_shapes_return_none() {
        assert!(detect(USDC_SAC, &[], &pubnet()).is_none());
        assert!(detect(USDC_SAC, &transfer_topics("not an asset"), &pubnet()).is_none());
        assert!(parse_asset("TOOLONGASSETCODE:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN").is_none());
        assert!(parse_asset("USDC:not-an-account").is_none());
    }
}
