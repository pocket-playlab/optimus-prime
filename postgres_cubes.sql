--
-- Name: cubes; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE cubes (
    id integer NOT NULL,
    name character varying
);


ALTER TABLE cubes OWNER TO postgres;

--
-- Name: cubes_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE cubes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE cubes_id_seq OWNER TO postgres;

--
-- Name: cubes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE cubes_id_seq OWNED BY cubes.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY cubes ALTER COLUMN id SET DEFAULT nextval('cubes_id_seq'::regclass);


--
-- Data for Name: cubes; Type: TABLE DATA; Schema: public; Owner: postgres
--

INSERT INTO cubes (id, name) VALUES (1, 'jungle');


--
-- Name: cubes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('cubes_id_seq', 1, true);
