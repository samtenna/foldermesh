use libp2p::{
    Multiaddr, PeerId, Swarm,
    futures::{
        Stream,
        channel::{mpsc, oneshot},
    },
    identity, mdns, noise, ping, request_response,
    swarm::NetworkBehaviour,
    tcp, yamux,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use tokio;

use crate::sync::engine::Item;

pub fn new(
    secret_key_seed: Option<u8>,
) -> Result<(Client, impl Stream<Item = Event>, EventLoop), Box<dyn Error>> {
    let id_keys = match secret_key_seed {
        Some(seed) => {
            let mut bytes = [0u8; 32];
            bytes[0] = seed;
            identity::Keypair::ed25519_from_bytes(bytes).unwrap()
        }
        None => identity::Keypair::generate_ed25519(),
    };
    let peer_id = id_keys.public().to_peer_id();

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|_| ping::Behaviour::default())?
        .build();

    //swarm.behaviour_mut()

    let (command_sender, command_receiver) = mpsc::channel::<Command>(0);
    let (event_sender, event_receiver) = mpsc::channel(0);

    Ok((
        Client {
            sender: command_sender,
        },
        event_receiver,
        EventLoop::new(swarm, command_receiver, event_sender),
    ))
}

// Transfer types to be sent over wire
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StateRequest {
    RequestFullState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StateResponse {
    // flattened tree state
    FullState(Vec<Item>),
}

#[derive(NetworkBehaviour)]
pub struct FoldermeshBehaviour {
    pub ping: ping::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub request_response: request_response::cbor::Behaviour<StateRequest, StateRequest>,
}

#[derive(Clone)]
pub struct Client {
    sender: mpsc::Sender<Command>,
}

pub enum Event {}

pub struct EventLoop {
    swarm: Swarm<FoldermeshBehaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
}

impl EventLoop {
    fn new(
        swarm: Swarm<FoldermeshBehaviour>,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            swarm,
            command_receiver,
            event_sender,
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => match event {
                    _ -> {}
                },

                Some(command) = self.command_receiver.next() => match command {
                    _ => {}
                }
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    // start listening for messages from other peers
    StartListening {
        addr: Multiaddr,
        sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    Dial {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    RequestState {},
}
