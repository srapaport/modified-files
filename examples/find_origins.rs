use std::collections::{HashMap, HashSet};
use std::{path::PathBuf, time::Instant};
use csv::WriterBuilder;
use serde::Serialize;
use swh_graph::{graph::*, NodeType};
use swh_graph::{graph::SwhBidirectionalGraph, mph::DynMphf, SwhGraphProperties};
use indicatif::{ProgressBar, ProgressStyle};

fn main() {
    let revs = vec![
        "swh:1:rev:430b4cf723a79b315eeca02e574d7bb64cd2733e",
        "swh:1:rev:1cde611e005b242be4d5473643d0749d624b8eaf",
        "swh:1:rev:8c6b1bede2ebbca6c1c16c673b8b6d4d80f57d45",
        "swh:1:rev:6142111870f24778d3309969e9f1f1f9963e932c",
        "swh:1:rev:f41b9c03c119b021161a47c0ece2aef9952d2d15",
        "swh:1:rev:610ff54f7923b6f2e6606a9a0230db937e3e14c9",
        "swh:1:rev:f148fb256c4f0e7a1e83f9bd6351fa8be20097ad",
        "swh:1:rev:2cca4796d97dbfeed59f49fac304f6ed62a3ff88",
        "swh:1:rev:30d20de5a3519c8ffc7929ccf45052da5fdfabf0",
        "swh:1:rev:b4174057a91ec56e8413103f14837a32e4c8b305",
        "swh:1:rev:347b0b7cd9ae298af38d65c5c4f58c6c03c18277",
        "swh:1:rev:4e3da16e2e5cafdcfb6b97e8db6f854d0660bc26",
        "swh:1:rev:771b1b7940b552a4cf1160ef8a16910901775fea",
        "swh:1:rev:34d93f9054ed37c6037bdadbca37872f94304d33",
        "swh:1:rev:aecdfc8054cd2179460273f5d8229b7a7f91a739",
        "swh:1:rev:a40ccaf70557e608d8b091eb25ab04477f99ce21",
        "swh:1:rev:2f3ab7f94d26971cb8d6f1abf9e66a5115754a16",
        "swh:1:rev:519c9e5f0064d32838fc7b875c1b42307b9d3c28",
        "swh:1:rev:bb3d1851dd15f9e33286376c3c10d0ea8155c83d",
        "swh:1:rev:5330c5b4a953494c44ba0ad0be7c4c1a6e80c9ea",
        "swh:1:rev:3f0af8bc4251d77c7c5625c3cc7be0a21b077f8f",
        "swh:1:rev:6986b30abcdd6a497076121c9d1697656f73ab2d",
        "swh:1:rev:8dc922ca7ed7e820e2eaf33734cde4d0fd5a5222",
        "swh:1:rev:64662afb7909e73b4936e3494e418532d5553aa8",
        "swh:1:rev:450619d47af1c73e1b16c3913ceeb6f3f022bb3a",
        "swh:1:rev:b08530ae7332d4f8ca2d9ad470ea651fd5e22ba5",
        "swh:1:rev:81424e4f9ecf76465672b67b68c032645478dff4",
        "swh:1:rev:2b570d9c6d5f067aae7cf49444b2f65ca79e931d",
        "swh:1:rev:7db872f4926c96a8a1ced87918ec4486deac3daa",
        "swh:1:rev:5639f69ea8487897a1e85ee86cb2b1f46cff8d85",
        "swh:1:rev:7fecc30d48698c6ecc50804db2c7e4601439cf23",
        "swh:1:rev:a08ca440901b459ade2ae662b7a1ab8fdfca76d1",
        "swh:1:rev:e0cbf90f1ab3ba1ee726d2f8d326dbaebdad09f2",
        "swh:1:rev:aeb175890b99ce2b692a90268c093b1049da393e",
        "swh:1:rev:b9e38d5dd7bcb2b28025cba675fdd957e4985bd8",
        "swh:1:rev:19763a60884db09c8460f9c7603661fe250b3ff3",
        "swh:1:rev:7a9e242aa4ecf84b803a5d0c45a838e5c65ad5dc",
        "swh:1:rev:6fe7ab4769495f2bdc7a92a897d51e0daed60b0f",
        "swh:1:rev:7384f0bbb133d0e6f25c5c5c909dd7bb3ca78db2",
        "swh:1:rev:654f4568efc72cb28f4fdb7a75fad62361ace21b",
        "swh:1:rev:0bc2955ed88df80f6c72e969e7766b4eed297315",
        "swh:1:rev:1b00599788cb939f6d18d5ce3afd38a7e1b521f6",
        "swh:1:rev:1fc4b6e451e3c15f3a13e5ca99fd4c6f8d3e5f8d",
        "swh:1:rev:722aa28c200a1233c54f6a6af5802ed4cd68cb22",
        "swh:1:rev:5ea262ea708d2fc0a8472b318b42edb84e0c31ea",
        "swh:1:rev:36ee464e3b41027f8d2ee0c6345df1837f293b2e",
        "swh:1:rev:8eb47d52af5fd6969247fecfdb8ba6d0dd679361",
        "swh:1:rev:6c9262cbf228a36bc7d20b46536a6033752e6d7d",
        "swh:1:rev:88312228673d8aec16de8ecc945d72a37b9e4dc3",
        "swh:1:rev:5a7e698e2522074927ac73176885e316311c8856",
        "swh:1:rev:2824c2d228ebb4d8d27c9432dfb54531209d7c4c",
        "swh:1:rev:7f21c890ec85eee51c64d9f6b6cd5faf8bcce685",
        "swh:1:rev:9f8b6414a92f6a2d99e1b2efe33f37d7adcc8fbe",
        "swh:1:rev:32ba6b9207a943e051a960ceefc789bec03c6deb",
        "swh:1:rev:e8ef1cc222fdcfa21c2d2a4e49fc38250cc84322",
        "swh:1:rev:05213a301ec1f02ad95655ebd960922a4156ca5b",
        "swh:1:rev:fe65811725ba1363ad915e401c2b809e2fc54ba3",
        "swh:1:rev:077b44949f3f4bbd3f241d167e4a797276696909",
        "swh:1:rev:6735441e04e8f3328ca581afe3caeaa9cee30b90",
        "swh:1:rev:5f0145f5eb723cd5290c1985838422c8157ae7b5",
        "swh:1:rev:0589a4dd3dc3b2db600cac9be62562e92a41a2ff",
        "swh:1:rev:ca83d7bf7e671071e01d49c8d877b1cf36cdcec7",
        "swh:1:rev:7b0c8047d2ddbe00b60f5f8fe0b8db99a056214f",
        "swh:1:rev:5457301265222c654e4144fe1730aad93f723f81",
        "swh:1:rev:e24721c400c6bda152a512aee20e99ff873447cf",
        "swh:1:rev:fa8e371d99f18499d1c3614cc7aed2a8f57b5d32",
        "swh:1:rev:41f8ea14a377ded29670a0a3338c6f385aa4adc2",
        "swh:1:rev:f204a2332c18a90c30f27b9083face5bd3028811",
        "swh:1:rev:05d67c6e37897dcb02ec9d5d6d5d17387f1d2c39",
        "swh:1:rev:5ff8e447eb0f02a80cf757b2ed20a6f15864b5ff",
        "swh:1:rev:76355f7270537a8778fb430de6183f70cfc6f79a",
        "swh:1:rev:077bba9c2e21ed5ee6fa29203165b5ca6ee590a9",
        "swh:1:rev:70be16a8fe44c34febedcc47043a4d4ac0c68a03",
        "swh:1:rev:2359a950991a4520af19d452435c1fccef869283",
        "swh:1:rev:cd2e2d186d1bbf6c60e9ab9300027bb0b5ff5d3e",
        "swh:1:rev:9c72c13066784c734b96443d3b33320ba4e696de",
        "swh:1:rev:e62be0efe96c217b704c5a8cc9b8c62e3b814d69",
        "swh:1:rev:2767db88de8bab515a86e167a1052a3719ece72a",
        "swh:1:rev:f644c2cbd466cb2efe893a880ab2ba86ffd2bc65",
        "swh:1:rev:003d9e7b52c3a91cb584b3afd78cd06c539f13be",
        "swh:1:rev:b9a355b0f50e3ac37fb026dad987f63c5686b63c",
        "swh:1:rev:73b933ca52531b06b7120ec0daa797997bb43471",
        "swh:1:rev:2d079b79331bec7dbcbf490d69d0efeac7d598d9",
        "swh:1:rev:afc1ddc0feb759302faa71be45956d78961122f9",
        "swh:1:rev:eb816dca166a28219df827e693cb28c0b52d4385",
        "swh:1:rev:6a21ad9c3c18f7c400b2b9e0894becef096f2f65",
        "swh:1:rev:30f05860fdc5dcb8ae3801e3cd8c3dd83ec5f1c1",
        "swh:1:rev:5024670c871b45c86c8e37469f7d2fab550b7bbe",
        "swh:1:rev:d4665a7a810c4a32768f7e46f8976d4a604d7592",
        "swh:1:rev:28b40fb58b9ce1905e2ff333d737d321fb33215e",
        "swh:1:rev:6bd2f3f318beb1714bc01529b0e7d8457b836d52",
        "swh:1:rev:4f16db6cb42ab37fd102c7e87876b6e4d2987fbb",
        "swh:1:rev:1da266544e64ea862559a5fcc553fe988fb26795",
        "swh:1:rev:dd9c839abe933a82c4a426f3f8f0859e414f6638",
        "swh:1:rev:6950392610abfee0d90f5d14df736dd2cd11bfcc",
        "swh:1:rev:36a79675d41f064c6ca6e3a8067d6bca9aa42625",
        "swh:1:rev:178568c20b5979d569aad1bdf40e9774384dc44c",
        "swh:1:rev:b1b5d9b64d52cafa660945dd7ebf9dc1beff0556",
        "swh:1:rev:4737bedc2216c9141cdd042097af17038647990c",
        "swh:1:rev:d2da48f4fece258b64f689bd0ff20548d9e70b4d",
        "swh:1:rev:856871fb5085065870be53c10230f102d2ccc9d1",
        "swh:1:rev:25edddf701fbdf55ab4f436026771ad1e6314e5c",
        "swh:1:rev:37305ff324129ccb07527c861afffac2ea7874ea",
        "swh:1:rev:2ff9599c84fb561e1e5032a573121b81408ac224",
        "swh:1:rev:fa5fa8b344c2f458e7cdd523d2a810c2afec73ca",
        "swh:1:rev:abe1637848787489845c3f43a1190c5bff82ef83",
        "swh:1:rev:75a717d78c2b6e7e3f520fc50dbad45cda341701",
        "swh:1:rev:58df9168fd30d42d4acf27b764ed3e8118ecf01b",
        "swh:1:rev:9e8de4b636e390ef68035d07aa7a854ba5c6837f",
        "swh:1:rev:f9619c22b457882a750f41db8429138ecc42a06b",
        "swh:1:rev:b2ecb041dff4dab662f197cc835789f53afcf50d",
        "swh:1:rev:7b03987a95a89131b6b30d37fd9f7dc3b60cfe07",
        "swh:1:rev:50ef54636e9bdadb6a21b73fa352c48731051855",
        "swh:1:rev:e063219bc3d045c4ca1f5013c711bc4aacac9ea1",
        "swh:1:rev:fd4804967ed4776fee8c62b012dfa4c7231ffebb",
        "swh:1:rev:909a46ca8ce46a490e1e4ebe4219999d6612df44",
        "swh:1:rev:b128eefbd2701444c47115e1cdd7e21daada4a29",
        "swh:1:rev:816d0660a141f5d9aab3519affb4ae00176f3294",
        "swh:1:rev:2d1ad261aade55a47d635dbc2d4ed35a0188f7b7",
        "swh:1:rev:a875a507a93c43d49cec527ca1297280052b1713",
        "swh:1:rev:46d6b025d8f61f08e45629362d7c559cd37496ac",
        "swh:1:rev:7dbc0aefc3250d17985bea3511396edaf23e70e2",
        "swh:1:rev:ebdb4dbab5b56eb0e31087bcdc83ea4401b5ad7f",
        "swh:1:rev:687396ae2e022fa421cb147f6240c427ee88902a",
        "swh:1:rev:99bc89903c00b032313f146666af92158f2385cd",
        "swh:1:rev:bae9279856b2abd5789297e527be7a2ed81f6b50",
        "swh:1:rev:d198d745982eeb486c78c593e99f851cdbc67b62",
        "swh:1:rev:f9023e207603c6b386acf45a1fd4fb8171d244e6",
        "swh:1:rev:681107dea178f5e74807225d1e2e9c23dccf90d7",
        "swh:1:rev:ee4ac1aad0ec8ab0629677ea8938811ece6184ab",
        "swh:1:rev:5b2044775e0d1871da685868c76ec7a46f9555a3",
        "swh:1:rev:d0a06437a48b85efff4f2d3d8cc2f598f7d4570f",
        "swh:1:rev:dda046ebaa080a01be164c4a757d32b2d49d5649",
        "swh:1:rev:1d789417db67ebb7eb6e92fdce7c0b9555771da4",
        "swh:1:rev:c06fcc34142ca887ccf5baeaf3da84e5c227c463",
        "swh:1:rev:98645af254c61cf5d6bbf8b4b2c1aa781e920486",
        "swh:1:rev:ca0d1742c73d3d36792bf09927991c51c36f0209",
        "swh:1:rev:97ba74b259c1df7a2e1e72f585a35d0b92fc5dcd",
        "swh:1:rev:3479599b6aeb8cd3af2b2b08849b5e437477ea86",
        "swh:1:rev:c8c672c3acc5baedfd57caf926df7628b959bab1",
        "swh:1:rev:c8c31b3d42f9ed51bdcc4cd3ee1de74818655b51",
        "swh:1:rev:8eea4b898dd582f0f8cb3571c919ee77ac56daac",
        "swh:1:rev:189995a5663974b281a14d59f732c7250d456100",
        "swh:1:rev:c38c8f28b65b1abf483cc24f0417b87621c513ec",
        "swh:1:rev:d1844902c2ee3ac23e2907278a240acf06bd3580",
        "swh:1:rev:82c7f3b09ad9f1c03187ca6ecb30171fccaace60",
        "swh:1:rev:66593934fc752494aa9fdbc79e6743cc43a64116",
        "swh:1:rev:c7120eeb4decd8db776976bab7537b315a53d3fc",
        "swh:1:rev:9def7cc9eea7730ac4760e5852f8984164bbdad1",
        "swh:1:rev:cd4cb60aa186c82392aeb5821e56f3e4594d844c",
        "swh:1:rev:8265390a1e47494f7349b4b01252833c9464cebb",
        "swh:1:rev:e5a0f2ad47aa6dddde7a618cbee545bdf0c56ac3",
        "swh:1:rev:92ad398911701738bfb98edb67e7b3fdc880b00d",
        "swh:1:rev:b8d91c72b47d4da2514a5e20ac47fd33d8b8a717",
        "swh:1:rev:a5b641e4ce18adffbbcb1d6575432cac6ec51fd8",
        "swh:1:rev:551ac652287b00cdd6cc1c70750f569d9183432b",
        "swh:1:rev:27bbfa7c3456715b44f6510d184f302fed1537bd",
        "swh:1:rev:c658a7b1fb548b3988f4b4c1b4171e0537af7c5b",
        "swh:1:rev:d0669963c3cd7cd93e7c92195c10b308bd5b62cb",
        "swh:1:rev:7c9a7d3f45c6c0a4ea2af54df3b142a6d8d34400",
        "swh:1:rev:f2797164793110355930f4622caa12d0fd4de7f4",
        "swh:1:rev:96e571c8b571bb337d7cf4eaeb62bbb2a05a86b4",
        "swh:1:rev:bc92000be019e9a5a48b3c94b2466f0d3cb60597",
        "swh:1:rev:980ac58ad9cca962fc11549439ee4873471bde66",
        "swh:1:rev:19be8f74d711e5ed5b0322014b35c73f8833dbd1",
        "swh:1:rev:1f3e4963775d5da8cc02e77e3bf06a59dfdcae23",
        "swh:1:rev:d0c3ae40a5584bd04f08abd70e53c8e1c7c3ca4d",
        "swh:1:rev:f0f8da61bb780e81b3766ad6d2c1f674b2314585",
        "swh:1:rev:e895d7b789b2eb54b4e934ade24c8c51ecef0726",
        "swh:1:rev:4ffbee8b9a4408b44456197a307d630bb29e3123",
        "swh:1:rev:eb0c80e18ea10796e4d17c1e99c658ee4cd3cdaa",
        "swh:1:rev:1db2bf8352523c3df1b64ff635f44a0e576660a3",
        "swh:1:rev:74256f235b84c354978e10de5d870390f5281e53",
        "swh:1:rev:c8bcd9b3f0031045f73d03f40b27fda856593bdc",
        "swh:1:rev:b49d3353ce8a5a1b3aae6564a62bfd46ac35aecc",
        "swh:1:rev:dbf6f98b9ac2b0723f9b99736a4adb08a9b4aaa2",
        "swh:1:rev:f7ed7efae0361a4a99c9b65def3b6b8cdfd18288",
        "swh:1:rev:05a15f651f5a325b6d21b2084ec14cbfec24c762",
        "swh:1:rev:d5182f871cd4224afdc4f1c681e48ddde42ef1e6",
        "swh:1:rev:4521bcd5eb087386bea163c429ef03612df6b9e3",
        "swh:1:rev:82993690ff4bbfc0cf7c0f61fadc7d52836e827a",
        "swh:1:rev:b886fffe54b5f2028a1f0056ea12a4132bf89e26",
        "swh:1:rev:81b9b17cdeed5e3e58e31f4db03bddc95da34a58",
        "swh:1:rev:4439273d0051cea7d50ab01e831f2bca34102040",
        "swh:1:rev:0ce4d84b99ea071d7073f0e9b9c2942796545e58",
        "swh:1:rev:bbc1feb3c09bace83499cfdf54d131a66cde3b03",
        "swh:1:rev:e046c54a7c0426cbf0e14444be61cc4ba66fb10c",
        "swh:1:rev:0b2d2d68e20864c3a2a37572229f7f4427905d6a",
        "swh:1:rev:6273da517ec106eddccb4f052cfd94ad7b2a8d98",
        "swh:1:rev:7b14e89ccd79ed2c876439b3a1b1b2e0e4e44262",
        "swh:1:rev:fbf265518ed10a96a9657d27886d6be90985814f",
        "swh:1:rev:504b3099d6921662458a755f2156d7b0b74bc2dd",
        "swh:1:rev:afe1c9b737447298329004d3ced5b883fed9d93d",
        "swh:1:rev:7a5d28d6f9521e8eb0961b7b4cf23f2471df1619",
        "swh:1:rev:917445b7803c948b98db088ca546bc8c0e5fdba1",
        "swh:1:rev:178bb843c5fdbc614d714b3c0181c1f860b77f06",
        "swh:1:rev:cc238c060bd59347cacdead0fe2103c3303164a0",
        "swh:1:rev:86d9d44da0d61d6dac496be9b6df11f3e009e26e",
        "swh:1:rev:9a7befb13176f007e987ce2d017a2673acbdedcf",
        "swh:1:rev:783d343e7d0c9d5a948ca61eaa2d343a10c8b308",
        "swh:1:rev:f18aefbae3c197087399cbe1a95b3a28ec842f4f",
        "swh:1:rev:99f1dea08084d1d33143807e832f61a7ea83df04",
        "swh:1:rev:130a614a5e8c2793bcff931ed98d53fed8c475df",
        "swh:1:rev:7ed368421c6fda3c472c2735715a8aaf0b546b2b",
        "swh:1:rev:91e7cf6f0304b234501ac7b13bdb2ff5b12295ea",
        "swh:1:rev:f0e2bf4884f7cff5d5e32603e5c9510c067f90b7",
        "swh:1:rev:9628541ad97e86e791b689bece06ffa612caf285",
        "swh:1:rev:26449a298293170da1a97e419f677c31d9dd8677",
        "swh:1:rev:adb274cddb8caf845d040d48ee4f2094487e4bbe",
        "swh:1:rev:cce1bd05c73635281bfd8c214b96d283ab070457",
        "swh:1:rev:eab6c839f20fc007daf3fb4d09f6a1e2d6587e21",
        "swh:1:rev:292ca5aa7733f7c591718d08e9082128747eca90",
        "swh:1:rev:4a24268da273b3303e5015680dc1d5d6c19c28a9",
        "swh:1:rev:2412084366d623191fd292853b72b47141b7b364",
        "swh:1:rev:140a3a74df3feae9dacb2d40f0b252e3c8224639",
        "swh:1:rev:5a08692980f8568d6ef05b8d207a1114080aaf86",
        "swh:1:rev:1c303e762cc4e140546ee781ef1f7dd0088238ba",
        "swh:1:rev:712140b35ce8b26baf9ee9e6488c99f7daf3cf0b",
        "swh:1:rev:d1bec88b0e0df51c8af6eb607ba21e957f0fe296",
        "swh:1:rev:4d35a2522fd97f51bfe834c2aabb33482c19cba0",
        "swh:1:rev:4d712b2e4e579a907858ea69b52cd1948671b053",
        "swh:1:rev:61f12f2e8ff22bcf092d1a43d3c754673711f4eb",
        "swh:1:rev:f118713df9a4d30349a87055c095db0a1df6600c",
        "swh:1:rev:0cb3837c390ac51a9710522fcbb7809f960b3243",
        "swh:1:rev:dc2a982540d04842a93a695c03ca2a730fc04a7e",
        "swh:1:rev:c32d01c89c2074a921cb3988a22f2c06610e67c1",
        "swh:1:rev:7ac4ca41497c2b7ba385863faf385ad37bc078e3",
        "swh:1:rev:14a50050fdee1b44a8ed76e721d74299e13bb23b",
        "swh:1:rev:0b3d5382df2f705f83eb7d9986d7ca1ad7109e51",
        "swh:1:rev:c6e14b04b36dc84dbc587cab55d86fd9181e7a54",
        "swh:1:rev:00927e76d7c27a5625a9eec9f1dde92fc4776419",
        "swh:1:rev:88e1c75334f7ce02d635581289eaa3bc1a68c591",
        "swh:1:rev:bfe04cbfc7eaccb26c408336183f0abecb5a1417",
        "swh:1:rev:f6abe3a336e7699a29b11c0cbba374a5ccae9aa3",
        "swh:1:rev:07d766cb10c5fe017061e64d866b67c2174fff13",
        "swh:1:rev:5f47668c44b058233c7511b567b8637e3b5e6676",
        "swh:1:rev:1c6758792d65973639d3e390d075c502eedb74b4",
        "swh:1:rev:380217b0e08dfc75fff6a9990f15548fa4d40f13",
        "swh:1:rev:276251abfabe92b2dd36e7cb823f5c336eefb1f5",
        "swh:1:rev:4ce3b4447810ce47b3fe57f948810e1e8d3ec4cf",
        "swh:1:rev:634d9cf7df3ea08b58463facf291da25c46bd9e1",
        "swh:1:rev:fad4a7f630b145235585f77556f342a51e3b7534",
        "swh:1:rev:e0a283226289e52911f2c69036a7ea339b7dd2a3",
        "swh:1:rev:08942d38548e7a0b613dba113315e4d0d089daca",
        "swh:1:rev:9c680a580dcee4310fa745f1770831f4fd83e082",
        "swh:1:rev:73017d052dd2d5591ad9163e7810ee25e57f4a21",
        "swh:1:rev:9d7e94f3a575938bcf8e42b9bbf52e26463b2231",
        "swh:1:rev:1694e53ae8fb3b611c5ddcb8cfa84faaa74d266d",
        "swh:1:rev:6f3a5f2e87c16650fa442eb1ffb1e523efa36392",
        "swh:1:rev:43cb93bc18b752ef7d81ce5accad7a23cc00cd94",
        "swh:1:rev:bb2e7199a156df53c60a86aad2cbff3a59bb7285",
        "swh:1:rev:024a0d9f6af46f8c7331b9cbef7df604ebd36d76",
        "swh:1:rev:8b9afcb260f12077b42eff4f18507264c6f282bb",
        "swh:1:rev:21ebb3d30b515bec20f45a29d819cbf4cf8b65e1",
        "swh:1:rev:379d323f2aaf6527b1a60a7a1984e4e45ea7e6fa",
        "swh:1:rev:39dcd659066c97618801feefb2f6e70ee5075871",
        "swh:1:rev:9f47546f903344d9eb8158d0e6c6960c3b22a585",
        "swh:1:rev:ce02f1d1b5999afe263be354a8c32704febbbb88",
        "swh:1:rev:f11a94ca742cfd13e5b64e518ded78f1a7f119fc",
        "swh:1:rev:489a865421cb0eb79bd5198db5fb88d89362fa59",
        "swh:1:rev:2bde618250eff03a8a1c835347ca8bf887335d6d",
        "swh:1:rev:a9290f95c109ef81e39780f72fc8be8d89f2f42c",
        "swh:1:rev:bcb573e653144caab3486add38b480d44806f182",
        "swh:1:rev:f0e2e4a06ca540a300b2b0ec449c44d55c10f040",
        "swh:1:rev:37d6234ed4da2e7695039867872be8008b3b49a9",
        "swh:1:rev:65526a1f8adfe9e8183ec6cc7b0a199a56e48f22",
        "swh:1:rev:584c5c102c61b709459bff7a3bf492e1e98495f9",
        "swh:1:rev:b21bf8b1af775e17833da4432d48ffc18a294ebc",
        "swh:1:rev:88cc15cc13ad68fc204534e69f7e395e0a327c4b",
        "swh:1:rev:577c3d169cc7ee20c72a0d51a81989b562ab09af",
        "swh:1:rev:f6f6c9fcc7b236e34cf9551244c2b4b0115f41ec",
        "swh:1:rev:7ed0482630eea2f4c44b6db59e1d807da45d820e",
        "swh:1:rev:438f40be4722677240589c985d5c1631fb405317",
        "swh:1:rev:79c1a923e7b0af956f3a0edf275fd3a58eb99167",
        "swh:1:rev:d670f51fd027dc9e47fd2ad5303badfc884a48dc",
        "swh:1:rev:5f9619f4a337e9c6c0c19997d34230e35f65e129",
        "swh:1:rev:a7d4ea1b5eea1b57a50afbea6a459b90a86ba1af",
        "swh:1:rev:89729eac0e8c7b7f7eb2d25bc9d0c648fc96202f",
        "swh:1:rev:86758bbe4a1b6056158f3acbf18f3aefe93f041d",
        "swh:1:rev:6f4a0bd86c5aa30d38acd30e2812c9e3b1e09662",
        "swh:1:rev:71c84040a5fd0245097a8913fdddce849155677a",
        "swh:1:rev:0939a320318000109b3f56b5c1527af111e8939f",
        "swh:1:rev:5f61c8988e85ad77de20cc55301f1b9758ba8776",
        "swh:1:rev:315b31d7dd663cec3bfa52c1c17c05f4731bc14a",
        "swh:1:rev:e0d1c194443cd2c5797757948ac9905c2572a422",
        "swh:1:rev:38d7e01ce87c446596e398870bcca0ced51b9adf",
        "swh:1:rev:4536cb9fbe53d7ea3e913fac9219744c44351df1",
        "swh:1:rev:45bfbf39206d808c6dd74e2fe2e6413467737356",
        "swh:1:rev:75897c2dcd1dd3a6ca46284dd37e13d22b4b16b4",
        "swh:1:rev:e212aa1b73c469cc07dd09ec0be0e00161f9be63",
        "swh:1:rev:dbe5ce269fed6a022a16fa794be4e5216c7068cc",
        "swh:1:rev:b4e8672c8e0cddd44c9ac6409e93def0ed682f58",
        "swh:1:rev:64aa351b455bb4ab819c5500c0cc6447368413eb",
        "swh:1:rev:b6a119771a69c8103a2b81d7cc5d1d3cb2f38dc6",
        "swh:1:rev:348939273ffa32739b86b95ec6227d0d74d3070e",
        "swh:1:rev:3c761e43f8a8cda8858b1d3e08b76b97f422f7e3",
        "swh:1:rev:ec2a89f3c1eac10b7141c9e0dc43bca44d46e18b",
        "swh:1:rev:6bd2151d299adbc80905e5bf058f3b382cbb2010",
        "swh:1:rev:42f3773e828f6fc48cd5f476d9d4e15af938c892",
        "swh:1:rev:d6cad6fbe849e86ae31411fc3e614102f0c6c86d",
        "swh:1:rev:aaf28bbc064680102f0c918656f0ec0a64c9be49"
    ];
    let graph_t = SwhBidirectionalGraph::new(PathBuf::from(
        "/poolswh/softwareheritage/graph/2024-08-23/compressed/graph",
    ))
    .expect("Could not load graph")
    .init_properties()
    .load_properties(|properties| properties.load_maps::<DynMphf>())
    .expect("Could not load maps")
    .load_properties(|properties| properties.load_label_names())
    .expect("Could no load label names")
    .load_labels()
    .expect("Could not load labels")
    .load_properties(SwhGraphProperties::load_strings)
    .expect("Could not load strings")
    .load_properties(SwhGraphProperties::load_persons)
    .expect("Could not load persons")
    .load_properties(SwhGraphProperties::load_timestamps)
    .expect("Could not load timestamps");
    let start = Instant::now();
    /* #[derive(Serialize)]
    struct Row{
        origin: String,
        rev: String,
    }
    let mut csv_wrt = WriterBuilder::new().has_headers(true).from_path("results/origin_impacted.csv").unwrap();
    all_revs(revs, &graph_t).into_iter().for_each(|(rev, origins)|{
        origins.into_iter().for_each(|origin|{
            csv_wrt.serialize(Row{
                origin,
                rev: rev.clone(),
            }).unwrap();
        });
    });
    csv_wrt.flush().unwrap(); */
    find_origins("swh:1:rev:c6f8511a5366a1c1427feb6d3faffb148c833e05", &graph_t).into_iter().for_each(|ori|{
        println!("origin: {}", ori);
    });
    println!("time elapsed {:.2?}", start.elapsed());
}

fn all_revs<
    G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph + Sync,
>(
    revs: Vec<&str>,
    graph: &G,
) -> HashMap<String, HashSet<String>>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let mut res = HashMap::new();
    let bar = ProgressBar::new(revs.len() as u64);
    bar.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {eta}",
        )
        .unwrap(),
    );
    revs.into_iter().for_each(|rev|{
        res.insert(String::from(rev), find_origins(rev, graph));
        bar.inc(1);
    });
    bar.finish_with_message("Done");
    res
}

fn find_origins<
    G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph + Sync,
>(
    rev: &str,
    graph: &G,
) -> HashSet<String>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let mut origins = HashSet::new();
    let mut to_visit = vec![graph.properties().node_id(rev).unwrap()];
    let mut visited = HashSet::new();
    while let Some(node) = to_visit.pop() {
        if visited.contains(&node) {
            continue;
        }
        visited.insert(node);
        for pred in graph.predecessors(node){
            match graph.properties().node_type(pred){
                NodeType::Origin =>{
                    if let Some(msg) = graph.properties().message(pred){
                        if let Ok(url) = String::from_utf8(msg){
                            origins.insert(url);
                        }
                    }
                },
                _ =>{
                    to_visit.push(pred);
                },
            }
        }
    }
    origins
}
